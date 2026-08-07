# CI/CD status and open work

**How the two workflows actually work now lives in
[`.github/workflows/README.md`](../.github/workflows/README.md)** — branch/target mapping,
what each job does, authentication, service principal setup, and triage. Read that first.

This document is the other half: **what is configured today, what is broken, and what still
needs deciding.** It is an assessment of the proof of concept, not a how-to.

**Status: proof of concept.** The mechanics are built and running. Everything under
"What needs to be done" is not. This exists so the shape of the approach can be evaluated
before it is committed to — it is cheap to change now and expensive to change once a second
source system is on it.

Written 2026-08-03, against `dev` @ `0985685`. Verified against the live repository and
workspace configuration, not just the YAML.

---

## 1. Where the confidence actually comes from

Worth being explicit about, because it is the part that generalises to other projects:
`databricks bundle validate` is a much weaker gate than it looks. It passes green on a DDL
string that cannot parse, on a data-quality predicate naming a column that does not exist, and
on a `libraries.glob` that matches nothing. All three are string-typed things Databricks only
evaluates at pipeline *update* time.

So the real pre-merge gate is the pytest suite, and it is structured around exactly that
weakness — three layers asking three different questions (does the code do what we meant; does
the declared schema still match what the code emits; are the quality rules themselves
well-formed and effective). See `oxygen_medallion_pipeline/README.md#testing`. The
architectural point for CI/CD purposes: **the tests are the gate, `validate` is a lint.**
Anyone sizing a similar pipeline should budget accordingly.

The corollary is the known blind spot. `src/whoz_ingestion_etl/transformations/**` imports
`pyspark.pipelines`, which only exists inside a running Lakeflow pipeline, so those files
cannot be unit tested at all. Nothing in CI executes them. Auto Loader's filename glob, the
landing paths, and the pipeline `configuration` block are proved only by a deploy and a real
run — and today CI does neither.

## 2. What is actually configured right now

Verified against the GitHub API on 2026-08-03, because the YAML alone does not tell you this:

| Thing | State |
|---|---|
| Repository visibility | **Public** |
| Default branch | `dev` |
| Ruleset "Protect environment branches" | Active on `dev` and `main` — **not `test`** |
| — required approvals | **0** |
| — required status checks | `Unit tests (pytest)`, `Validate bundle` (non-strict) |
| — force-push / deletion | Blocked. No bypass actors. |
| GitHub Environments | `dev`, `test` exist. **`prod` does not.** |
| — protection rules on either | **None.** No required reviewers, no branch policy. |
| Secrets | 3, all **repository-level** (not environment-scoped) |
| Branch drift | `main` is **14 commits behind** `test`; `dev` and `test` have diverged (5 / 4) |
| Deploy history | `dev` deployed repeatedly and green; `test` last deployed 2026-07-29; **`prod` never deployed** |

`oxygen_medallion_pipeline/README.md` §CI/CD currently claims prod is "gated by the `prod`
GitHub Environment". It is not — the environment does not exist, and GitHub will create it
unprotected on the first push to `main`.

## 3. What needs to be done

Ordered by what I would fix first, not by effort.

### 3.1 Blockers before any `test` or `prod` deploy carries real data

- [ ] **Un-hardcode the landing paths.** All four `whoz.*.{source,schema}_path` values in
      `resources/whoz_ingestion_etl.pipeline.yml` are literal `/Volumes/oxygen_dev/...` instead
      of `${var.catalog}`-qualified. Every *table* name in the bundle is properly qualified;
      this is the one place the environment split leaks. Fix all four together — a half-fix
      leaves the two entities disagreeing about which environment they are in. The surrounding
      comment and `docs/adding_a_source_entity.md` step 10 both describe this as already
      half-solved; correct them in the same change.
- [ ] **Consequence of the above, live today:** the deployed `test` pipeline shares its Auto
      Loader `schemaLocation` directories with `dev`, byte for byte. The project's own docs
      state that two streams must never share a checkpoint directory. This is currently
      harmless only because nothing has landed files for `test`.
- [ ] **Second consequence:** `test` was switched to `mode: production` (commit `5cec885`),
      which *unpauses* the daily `whoz_ingestion_refresh` trigger. So the `test` deployment is
      scheduled to read dev's volume through dev's checkpoint on a daily timer. The README
      still says all three non-prod targets are `mode: development`; that is now wrong.
- [ ] **Apply Unity Catalog grants to `oxygen_test` and `oxygen_prod`.** They exist only on
      `oxygen_dev`. The privilege list and the two traps are documented in
      [`.github/workflows/README.md`](../.github/workflows/README.md) → *Setting it up*, step 4.
- [ ] **Move the `prod` target's `root_path` off a personal workspace folder.** It is currently
      pinned to `/Workspace/Users/ikonstantinidis@sqli.com/.bundle/...`. Production deploy
      state should not live in one person's home directory — it disappears when they do.

### 3.2 The pipeline is not actually gated the way it reads

- [ ] **Add `test` to the branch ruleset.** It is the one environment branch anyone can push
      to directly, with no PR and no required checks. Given `test` is the tier that is supposed
      to prove a release, this is backwards.
- [ ] **Create the `prod` environment and put required reviewers on it.** Right now nothing
      stands between a merge to `main` and a production deploy. This is the single highest-value
      config change in the list and it takes two minutes in the UI.
- [ ] **Raise required approvals above 0** on `main` at minimum. A single person can currently
      open and merge their own PR to production.
- [ ] **Scope the secrets to environments.** All three secrets are repository-level, so any
      workflow on any branch can read the credentials that deploy production. Moving them to
      environment secrets (one SP per catalog, ideally) gives real blast-radius separation and
      makes the SP's grants a meaningful boundary rather than a formality.
- [ ] **Set `deployment_branch_policy`** on each environment so `prod` can only ever be deployed
      from `main`.
- [ ] **Add `permissions: contents: read`** to both workflows. They currently inherit the
      default `GITHUB_TOKEN` scope, which is broader than either needs.

### 3.3 The required-status-check trap

The ruleset requires `Unit tests (pytest)` and `Validate bundle`, but the CI workflow is
path-filtered to `oxygen_medallion_pipeline/**`. A PR that touches only the root `README.md`,
this document, `.github/workflows/README.md`, or `databricks-cd.yml` **never triggers CI, the
checks never report, and the PR is blocked forever** waiting for a status that will not arrive.

This has not bitten yet only because every PR so far has touched the pipeline folder. Standard
fixes, in order of preference: drop the path filter (CI is 50 seconds — the filter is not
buying much), or add a companion workflow with the inverse `paths-ignore` that reports the same
two check names as trivially successful.

### 3.4 What the pipeline does not do yet, and probably should

- [ ] **Nothing runs after a deploy.** CD deploys and stops. Given that the entire
      `transformations/` layer is untestable locally, the first real signal that a deploy is
      sound comes from a human clicking "run" in the workspace. A post-deploy
      `databricks bundle run whoz_ingestion_etl` in `test`, with the run's data-quality metrics
      checked against a threshold, would close the biggest hole in the current design — it is
      the only thing that can catch a wrong Auto Loader glob, a bad landing path, or a
      `libraries.glob` that silently matched nothing.
- [ ] **No rollback story.** "Redeploy an older commit" is the only answer today, and it does
      not address data already written by a bad run. Worth deciding deliberately: for a
      medallion pipeline the honest answer may be "roll forward, restore from Delta time
      travel" — but that should be written down, not improvised.
- [ ] **No smoke test that the deployed graph is complete.** `libraries.glob`'s
      `transformations/**` has to recurse into `bronze/` and `silver/`; the CLI only rewrites
      the glob to a workspace path and the match happens server-side. A non-recursive match is
      not an error — it is a pipeline quietly missing tables. Counting datasets in the update
      graph is a one-line assertion once something runs the pipeline in CI.
- [ ] **`ruff format --check` is in the runbook but not in CI.** Either add it or drop it from
      the runbook.
- [ ] **Pin the GitHub Actions to commit SHAs**, not tags. Minor, but this is a public repo.
- [ ] **Plan for OAuth secret rotation.** The SP secret was created 2026-07-28 and will expire.
      Nothing currently warns before it does; the failure mode is every deploy breaking at once.
      The rotation procedure is in
      [`.github/workflows/README.md`](../.github/workflows/README.md) → *Rotation*.

### 3.5 Structural questions I would want the architect to rule on

These are genuine design choices, not defects — I have picked defaults for the POC and they
deserve a second opinion.

1. **One workspace for all three environments.** `dev`, `test` and `prod` all point at
   `adb-7405610661645830`, isolated only by catalog. This keeps the POC cheap and makes
   promotion a one-variable change. It also means a workspace-level misconfiguration, a runaway
   serverless cost, or a compromised token reaches production. Is catalog-level isolation the
   intended production boundary, or is a separate prod workspace expected?
2. **`dev` is `mode: development`, deployed by CI.** That means resources carry the deploying
   identity's `[dev …]` prefix and land under that identity's bundle root. A developer running
   `databricks bundle deploy -t dev` from a laptop therefore creates a *second*, parallel set
   of resources rather than updating CI's. Shared `dev` should probably be `mode: production`
   like `test` — or the personal `local` target should be understood as the only sanctioned
   sandbox and `-t dev` blocked by convention.
3. **Three long-lived environment branches.** The alternative is a single `main` with tags or
   manual approvals driving promotion (deploy the same commit to test, then prod). The branch
   model is easy to reason about but it has already drifted — `main` is 14 commits behind
   `test`, and `dev` and `test` have diverged in both directions, which means "what is in prod"
   is not currently answerable from a branch name. That drift is the argument against the model.
4. **Public repository.** The workspace URL and the CI service principal's applicationId are
   committed. Neither is a credential, but combined with a public Actions history it is more
   reconnaissance surface than a client project usually wants. It also means fork PRs get no
   secrets, so `Validate bundle` cannot pass on them — which matters if external contribution
   is ever expected.
5. **Where does a second source system land?** The bundle is deliberately built for many
   sources (one `resources/*.pipeline.yml` pair and one `src/` folder pair each, no second
   `databricks.yml`). CI/CD as written scales to that without change — one validate, one deploy,
   all sources together. The question is whether that is wanted, or whether sources should be
   independently deployable, which would mean per-source workflows and path filters.

---

## 4. Summary for evaluation

What I think is right and worth keeping: branch-as-environment with catalog isolation; the
`local` default target; testing being the real gate rather than `bundle validate`; the
testability seam that makes that possible; the bundle being multi-source from day one.

What I think is genuinely unresolved: nothing verifies a deploy actually works (3.4); the
governance is weaker than the documentation claims (3.2); and the four hardcoded landing paths
mean the environment split is not yet real (3.1).

What I would do first, in one afternoon: fix the four paths, create and protect the `prod`
environment, add `test` to the ruleset, and add a post-deploy pipeline run in `test`.

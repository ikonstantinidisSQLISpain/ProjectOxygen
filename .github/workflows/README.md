# GitHub Actions workflows

Two workflows, one idea: **a branch is an environment.** CI checks a change before it merges;
CD deploys it after.

| Branch | Bundle target | Catalog |
|---|---|---|
| `dev` | `dev` | `oxygen_dev` |
| `test` | `test` | `oxygen_test` |
| `main` | `prod` | `oxygen_prod` |

Promotion is just a merge: feature branch → `dev` → `test` → `main`. There is no build step —
the deployed thing is the commit on the branch.

The bundle's `local` target (`oxygen_dev_<your-username>`) never appears in CI. That one is
personal, deployed straight from your terminal with a bare `databricks bundle deploy`.

---

## `databricks-ci.yml` — CI, on pull request

**Triggers on:** any PR into `dev`, `test` or `main` that touches
`oxygen_medallion_pipeline/**` or the CI file itself.

Runs two jobs **in parallel**:

| Job | What it does | Needs Databricks? |
|---|---|---|
| `Unit tests (pytest)` | JDK 17 + `uv sync --dev`, then `ruff check .` and `pytest` | No |
| `Validate bundle` | `databricks bundle validate -t <target>` | Yes |

The target comes from the PR's **base** branch — a PR into `main` validates `prod`. So you
find out a change breaks production config before it merges anywhere near production.

The tests need no credentials at all: they run against a local open-source PySpark session.

**Concurrency:** keyed on the PR number, `cancel-in-progress: true`. Push again and the
in-flight run is cancelled — a validation of superseded code is worth nothing.

## `databricks-cd.yml` — CD, on push

**Triggers on:** a push to `dev`, `test` or `main` (in practice, a merged PR) touching the
same paths.

One job, one command: `databricks bundle deploy -t <target>`, with the target derived from
the branch that was pushed.

The job runs inside a matching **GitHub Environment** (`dev` / `test` / `prod`), which is the
hook for required reviewers on production deploys.

**Concurrency:** keyed on the branch, `cancel-in-progress: false`. Two merges in quick
succession **queue** rather than race — a deploy killed halfway leaves the workspace in an
unknown state, so it is never cancelled.

Nothing runs the pipeline after deploying. First real run is still a human clicking "run".

---

## Authentication

Both Databricks-facing jobs authenticate as the `sp-oxygen-cicd` **service principal** over
OAuth M2M (machine-to-machine). No human account and no personal access token is involved —
if the person who set this up leaves, CI/CD keeps working.

Three repository secrets carry the credentials:

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | `https://adb-7405610661645830.10.azuredatabricks.net` |
| `DATABRICKS_CLIENT_ID` | The service principal's **Application ID** (a GUID) |
| `DATABRICKS_CLIENT_SECRET` | An OAuth secret generated for that service principal |

The Databricks CLI picks these up from the environment automatically — that is why the
workflow steps just set `env:` and call `databricks bundle …` with no login step.

Note that even `validate` needs a live session — the CLI resolves workspace state to validate
against, so it is not a purely offline check.

### Setting it up (one-time)

This is a manual handoff in two halves: Databricks **mints** the credential, GitHub **stores**
it. Nothing connects the two automatically — you copy two strings across.

**1. Create the service principal in Databricks.**
Workspace → ⚙️ **Settings** → **Identity and access** → **Service principals** → **Manage** →
**Add service principal**. Give it a name (`sp-oxygen-cicd`). Databricks-managed is fine; you
do not need an Entra ID app for this.

**2. Give it the workspace entitlements it needs.**
On the service principal's **Configurations** tab, tick **Workspace access**. (Bundle deploys
write to the workspace file tree, so it needs to be able to log in at all.)

**3. Generate an OAuth secret.**
Open the service principal → **Secrets** tab → **Generate secret**. Set a lifetime.

> ⚠️ **The secret is shown exactly once.** Copy both the *Client ID* (= Application ID) and the
> *Client secret* right now — you cannot retrieve the secret later, only generate a new one.

**4. Grant it Unity Catalog privileges — on every catalog it deploys to.**
Deploying and running the pipeline needs, per catalog (`oxygen_dev`, `oxygen_test`,
`oxygen_prod`):

```
USE_CATALOG, USE_SCHEMA, CREATE_SCHEMA, CREATE_TABLE,
CREATE_MATERIALIZED_VIEW, MODIFY, SELECT, READ_VOLUME
```

Two traps that cost real time here:

- **`CREATE_TABLE` does not cover materialized views.** Without
  `CREATE_MATERIALIZED_VIEW`, the deploy succeeds and then the *pipeline run* fails with
  `PERMISSION_DENIED` on `bronze.whoz_profiles_payload_shapes`.
- **`grants update` rejects the display name.** `--principal sp-oxygen-cicd` returns
  "Could not find principal". Pass the **applicationId GUID** instead.

Also worth knowing: tables end up **owned by the service principal**, so even the catalog
owner needs an explicit `SELECT` grant to query them afterwards.

**5. Store the credentials in GitHub.**
Repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**.
Add the three secrets from the table above, spelled exactly as listed — the workflows
reference them by name and a typo surfaces as a confusing auth error, not a missing-secret
error.

They are currently **repository-level** secrets, so every workflow on every branch can read
the credentials that deploy production. Moving them to environment secrets (ideally one
service principal per catalog) is the recommended hardening step — see
[`docs/cicd.md` §3.2](../../docs/cicd.md).

### Rotation

The OAuth secret expires. When it does, **every deploy breaks at once**, and nothing warns you
beforehand. To rotate: generate a new secret (step 3), update `DATABRICKS_CLIENT_SECRET` in
GitHub, then delete the old one in Databricks. The client ID does not change.

## When something goes red

- **`Unit tests` failed** — a real code regression. Reproduce locally with `uv run pytest`
  from `oxygen_medallion_pipeline/`.
- **`Validate bundle` failed** — malformed `databricks.yml` or `resources/*.yml`, or the
  service principal lost access to the target catalog. Reproduce with
  `databricks bundle validate -t <target>`.
- **`Deploy` failed** — usually Unity Catalog grants missing on that catalog. The bundle
  validated fine, so look at permissions before looking at YAML.

One caveat worth knowing: `bundle validate` is a lint, not a guarantee. It passes on DDL that
cannot parse and on quality rules naming columns that do not exist — Databricks only evaluates
those strings when the pipeline actually runs. **The pytest suite is the real gate.**

---

This file describes how CI/CD **works**. For what is actually configured today, the known
gaps, and the open design questions, see [`docs/cicd.md`](../../docs/cicd.md).

One gap worth knowing about while reading this file: both workflows are path-filtered to
`oxygen_medallion_pipeline/**`, but `Unit tests (pytest)` and `Validate bundle` are *required*
status checks. **A PR that touches only docs never triggers CI, so those checks never report
and the PR cannot merge.** See [`docs/cicd.md` §3.3](../../docs/cicd.md).

# oxygen_medallion_pipeline

Databricks asset bundle for Project Oxygen's medallion pipelines. One bundle, multiple
sources: each source is a self-contained pipeline + refresh job under `resources/` and
`src/`, sharing this bundle's targets, catalog variables, permissions, and CI/CD.

Sources today:

* **Whoz** — three export files ingested by one pipeline: **profile**, **talent** and
  **user**. See "The Whoz source" below, which has a section per entity.

To add a new source, add `resources/<source>.pipeline.yml` + `resources/<source>.job.yml`
(picked up automatically by `databricks.yml`'s `include: resources/*.yml`) and the
`src/<source>_etl/` + `src/<source>/` folder pair described below. No new bundle, no new
`databricks.yml`.

* `src/`: Python source code, **two folders per source** — the pipeline and the shared
  package it calls. The split is a testability seam, not tidiness: `pyspark.pipelines`
  only fully exists inside a running Lakeflow pipeline, so anything importing it is
  unreachable from pytest.
  * `src/whoz_ingestion_etl/`: the pipeline — only what Lakeflow loads and runs.
    `transformations/` holds the `@dp.table`-decorated dataset definitions in `bronze/`
    and `silver/` subfolders — layer-first, so a future `gold/` that joins across
    entities has a peer folder to live in. These files are thin wrappers: read the
    upstream table, call a `whoz_ingestion` function, return the result.
  * `src/whoz_ingestion/`: everything shared, imported by the pipeline **and** by the
    test suite — `shaping/<entity>.py` (the pure DataFrame-in/DataFrame-out row logic),
    `shaping/<table>.py` (one module per exploded child table, named for the entity that
    owns it, each returning importable SQL text), `schemas/<entity>.yml` + `contract.py`
    (each table's columns as data, rendered into the DDL string Lakeflow wants),
    `checks/<dataset>.yml` + `checks.py` (the data quality checks, as native Databricks
    Labs DQX check lists) and `dq.py` (the pipeline's one lazily-built DQEngine).
    Nothing here imports `pyspark.pipelines`, which is what keeps it testable; see
    Testing below.

    A sibling of the pipeline folder rather than a subfolder of it, because it has two
    consumers and belongs to neither. It resolves under the same name in both: the
    pipeline's `root_path` is `src` and `pyproject.toml`'s `pythonpath` names that same
    folder, so `whoz_ingestion.shaping.profile` is one import path in a running pipeline
    and in pytest alike — which is what makes a wrong prefix fail locally instead of at
    deploy time. Keep those two settings in step.
* `resources/`:  Resource configurations (jobs, pipelines, etc.), one pair per source.
* `docs/`: **Start with `docs/how_it_works.md`** — the mental model behind the folder split,
  the three test layers and the quality checks, and the one place that explains *why* the
  pipeline is shaped this way. Then `docs/adding_a_source_entity.md` (the step-by-step
  runbook for ingesting a new export) and the per-entity source analyses,
  `docs/whoz_profile_data_model.md` and `docs/whoz_user_data_model.md`, which hold the field
  inventory and every type hazard behind the casts.
* `tests/`: The test suite — one folder per layer (`layer1_shaping/`, `layer2_contract/`,
  `layer3_rules/`), entity in the filename. See Testing below.
* `fixtures/`: Sample source records the tests run against, one folder per source entity.

## The Whoz source

Whoz exports several files. Each is its own **entity**: its own Auto Loader stream, its
own schema location, its own bronze and silver tables, its own fixtures and tests. One
pipeline, `whoz_ingestion_etl`, models all of them — they share the pipeline, not their
tables. Three today, each with its own section below; a fourth would get a fourth section
rather than extra rows in an existing table.

| Entity | Export | Section |
|---|---|---|
| profile | the profile report | [Profile](#profile) |
| talent | the talent report (carries a nested profile object) | [Talent](#talent) |
| user | the user report — login accounts, **not** people | [User](#user) |

What is true of all of them is at the end of this section, under "Common to every Whoz
entity" — read it once rather than per entity.

### Profile

The profile export: one record per Whoz profile.

| Layer  | Table | Grain |
|---|---|---|
| bronze | `bronze.whoz_profiles` | one row per profile, full JSON in a VARIANT `payload` |
| bronze | `bronze.whoz_profiles_payload_shapes` | one row per distinct top-level key set (drift monitor) |
| silver | `silver.whoz_profiles` | one row per profile, **current state only** — AUTO CDC (SCD1) upsert by `profile_id` |
| silver | `silver.whoz_profile_history` | one row per (profile, version), `__START_AT`/`__END_AT` validity — AUTO CDC (SCD2), full history |
| silver | `silver.whoz_profile_completion_rules` | (profile, completion rule) |
| silver | `silver.whoz_profile_aptitudes` | one row per declared skill |
| silver | `silver.whoz_profile_positions` | one row per job/mission |
| silver | `silver.whoz_position_aptitude_refs` | (position, aptitude) bridge |
| silver | `silver.whoz_profile_skill_ratings` | legacy `skillRatings` — verify before use |
| silver | `silver.whoz_profiles_quarantine` | rows any DQX check fired on, + `_errors`/`_warnings` |
| silver | `silver.whoz_profile_aptitudes_quarantine` | ditto, for aptitudes |
| silver | `silver.whoz_profile_positions_quarantine` | ditto, for positions |
| silver | `silver.whoz_position_aptitude_refs_quarantine` | ditto, for the bridge |

The four `_quarantine` tables are not dead-letter queues: a `warn`-level row is in the
quarantine table **and** in the silver table. See "Data quality checks" below for what they
contain and how to query them.

Code: `transformations/bronze/whoz_profiles.py` for bronze, and one file per silver table —
`transformations/silver/whoz_profile.py` (the profile grain and both AUTO CDC flows), plus
`whoz_profile_aptitudes.py`, `whoz_profile_positions.py`, `whoz_position_aptitude_refs.py`,
`whoz_profile_skill_ratings.py` and `whoz_profile_completion_rules.py` alongside it. The row
logic is in `whoz_ingestion/shaping/profile.py` and one `shaping/profile_<collection>.py` per
child table. Landing config keys: `whoz.profiles.source_path`, `whoz.profiles.schema_path`.

The field inventory and every type hazard behind the casts are in
`docs/whoz_profile_data_model.md`. Read it before changing any of them.

### Talent

The talent export: one record per Whoz talent — a *person* in a workspace, where a
profile is that person's CV-like content. A separate file with its own filename glob and
its own schema location, not a variant of the profile export.

| Layer  | Table | Grain |
|---|---|---|
| bronze | `bronze.whoz_talents` | one row per talent, full JSON (incl. the nested profile) in a VARIANT `payload` |
| bronze | `bronze.whoz_talents_payload_shapes` | distinct key set × profile container type (drift monitor) |
| silver | `silver.whoz_talents` | one row per talent, **current state only** — AUTO CDC (SCD1) upsert by `talent_id` |
| silver | `silver.whoz_talent_versions` | one row per (talent, version), `__START_AT`/`__END_AT` validity — AUTO CDC (SCD2) |
| silver | `silver.whoz_talent_workspace_history` | (talent, workspace membership period), from the source's `history[]` |
| silver | `silver.whoz_talents_quarantine` | rows any DQX check fired on, + `_errors`/`_warnings` |
| silver | `silver.whoz_talent_workspace_history_quarantine` | ditto, for the workspace history |

Code: `transformations/bronze/whoz_talents.py`, plus one file per silver table —
`transformations/silver/whoz_talent.py` (the talent grain and both AUTO CDC flows) and
`whoz_talent_workspace_history.py`. Row logic in `whoz_ingestion/shaping/talent.py` and
`shaping/talent_workspace_history.py`. Landing config keys: `whoz.talents.source_path`,
`whoz.talents.schema_path`.

Two things about this entity that are not obvious from the table:

* **The two "history" tables are not the same thing.** `whoz_talent_versions` is how the
  talent *record* changed over time (AUTO CDC, derived). `whoz_talent_workspace_history`
  is which workspaces the talent has belonged to, exploded from the source's own
  `history[]` array.
* **The nested `profile` object is deliberately not re-modelled.** `silver.whoz_talents`
  lifts `profile_id` and a few cheap attributes and stops there; `silver.whoz_profiles`
  is the one source of truth for profile content. The raw nested object stays in bronze's
  VARIANT payload forever, so the decision is reversible without a re-ingest. The header
  of `whoz_ingestion/shaping/talent.py` records what a reversal would have to cover.

### User

The user export: one record per Whoz **login account**. A separate file with its own filename
glob and its own schema location.

> **A user is not a talent, and the two are not 1:1 in either direction.** `silver.whoz_talents`
> is a *person in a workspace*; this is the account they sign in with. Measured on the export
> analysed 2026-07-27: 1,760 of 4,036 accounts have no talent (service and administrative
> logins), and 1,841 of 4,116 talents have no account. Any headcount built on
> `silver.whoz_users` is counting **accounts, not people** — use the talent tables for people.

| Layer  | Table | Grain |
|---|---|---|
| bronze | `bronze.whoz_users` | one row per account, full JSON in a VARIANT `payload` |
| bronze | `bronze.whoz_users_payload_shapes` | distinct key set × the two role-map container types (drift monitor) |
| silver | `silver.whoz_users` | one row per account, **current state only** — AUTO CDC (SCD1) upsert by `user_id` |
| silver | `silver.whoz_user_versions` | one row per (user, version), `__START_AT`/`__END_AT` validity — AUTO CDC (SCD2) |
| silver | `silver.whoz_user_workspace_roles` | (user, workspace, role) |
| silver | `silver.whoz_users_quarantine` | rows any DQX check fired on, + `_errors`/`_warnings` |
| silver | `silver.whoz_user_workspace_roles_quarantine` | ditto, for the workspace roles |

Code: `transformations/bronze/whoz_users.py`, plus one file per silver table —
`transformations/silver/whoz_user.py` (the account grain and both AUTO CDC flows) and
`whoz_user_workspace_roles.py`. Row logic in `whoz_ingestion/shaping/user.py` and
`shaping/user_workspace_roles.py`. Landing config keys: `whoz.users.source_path`,
`whoz.users.schema_path`.

Three things about this entity that are not obvious from the table:

* **The join to talents lives on the talent side.** `silver.whoz_users` carries no
  `talent_id` — 44% of accounts would have a null one. Join from `whoz_talents.user_id`,
  which resolves to a known account on all 2,275 talents that carry it (0 orphans, verified).
* **`workspaceRoles` and `federationRoles` arrive as an object *or* an empty array**, the
  same polymorphism as the profile export's `completionDetails`. Workspace membership becomes
  the child table above; federation is flattened onto the account row because it is strictly
  one entry per record today, and `federation_count_at_most_one` is the check that fires the
  day that stops being true.
* **`silver.whoz_users_quarantine` is expected to hold exactly one `_errors` row.** The export
  carries a single all-null record, which `user_id_not_null` withholds. That is correct
  behaviour, not a defect to chase.

The field inventory, every measurement behind the casts, and what this entity deliberately
does *not* model (former usernames, agentic-studio roles) are in
`docs/whoz_user_data_model.md`. Read it before changing any of them.

### Common to every Whoz entity

`bronze`/`silver` schema names are literal and identical in **every** target — the
isolation boundary is the catalog, not the schema (see "Environments" below). Every
table name in `transformations/**/*.py` is built from `CATALOG`/`BRONZE_SCHEMA`/
`SILVER_SCHEMA` module-level constants (read from pipeline config via `spark.conf.get`),
never hardcoded.

Every export is a pretty-printed JSON **array**, not JSONL, and polymorphic in several
fields, so bronze reads it with `multiLine` + `singleVariantColumn` and silver casts
lazily with `try_variant_get`. `docs/whoz_profile_data_model.md` explains why in detail
for the profile export; the same reasoning applies to the others.

Landing folders are set per entity in the pipeline's `configuration` block in
`resources/whoz_ingestion_etl.pipeline.yml` — all entities share the one
`landing.source` volume and are separated by filename glob, but each has its **own**
`schema_path`, since two Auto Loader streams must never share a checkpoint directory.

> **Known open bug.** All **six** of those paths are hardcoded to `/Volumes/oxygen_dev/...`
> instead of being `${var.catalog}`-qualified, so a `test` or `prod` deploy reads *dev's*
> landing volume. It has not bitten yet because only dev has been run against real files.
> Fix them all together when you fix one — a half-fix is worse, since the entities would
> then disagree about which environment they are in. It was four paths before the `user`
> entity; that entity followed the existing pattern deliberately, so the fix stays one
> reviewable commit with one deploy test.


## Getting started

Choose how you want to work on this project:

(a) Directly in your Databricks workspace, see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code, see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With command line tools, see https://docs.databricks.com/dev-tools/cli/databricks-cli.html

If you're developing with an IDE, dependencies for this project should be installed using uv:

*  Make sure you have the UV package manager installed.
   It's an alternative to tools like pip: https://docs.astral.sh/uv/getting-started/installation/.
*  Make sure you have a JDK 17+ installed and on `PATH` (`java -version`) — PySpark
   needs it to run tests locally, see Testing below.
*  Run `uv sync --dev` to install the project's dependencies.


# Using this project using the CLI

## Environments

Four bundle targets, each pointed at its own catalog — `bronze`/`silver` schema names
are identical across all of them, so nothing about the pipeline's structure changes
between environments, only where it writes:

| Target | Catalog | Who deploys it, and how |
|---|---|---|
| `local` (default) | `oxygen_dev_<your-username>` | You, from your own terminal — personal, fully isolated, never touched by CI |
| `dev` | `oxygen_dev` | CI, on push to the `dev` branch (i.e. when a PR merges into it) |
| `test` | `oxygen_test` | CI, on push to the `test` branch |
| `prod` | `oxygen_prod` | CI, on push to the `main` branch |

**One-time setup, before your first local deploy:** your personal catalog needs to
exist first — a pipeline deploy can create schemas inside a catalog automatically, but
not the catalog itself. Create yours once:
```
$ databricks catalogs create oxygen_dev_<your-username> \
    --storage-root "abfss://oxygen-source@dbxpocstor3f4h.dfs.core.windows.net/"
```
(same storage account/container `oxygen_dev` itself already uses — no new cloud infra
needed). Replace `<your-username>` with your workspace short username (`ikonstantinidis`,
`miguel`, etc.) — must match what `${workspace.current_user.short_name}` resolves to for
you, since that's what `local`'s catalog variable is built from.

## Day to day

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

2. Deploy to your own sandbox — no `--target` needed, `local` is the default:
    ```
    $ databricks bundle deploy
    ```
    This deploys a pipeline named `[dev yourname] whoz_ingestion_etl`, writing only
    into your own `oxygen_dev_<you>` catalog. Find it under **Jobs & Pipelines** in the
    workspace.

3. Run it to actually exercise your changes:
   ```
   $ databricks bundle run
   ```

4. When ready, push a branch and open a PR into `dev`. `databricks-ci.yml` validates
   against the `dev` target; merging triggers `databricks-cd.yml` to deploy the shared
   `oxygen_dev` catalog. Promote the same way, `dev` → `test` → `main`, to reach
   `oxygen_test` and finally `oxygen_prod`.

   The `whoz_ingestion_refresh` job runs the pipeline daily in whichever target it's
   deployed to (`resources/whoz_ingestion_refresh.job.yml`) — paused in `local`/`dev`/
   `test` (all three stay in `mode: development`), running for real only in `prod`.

5. Finally, to run tests locally, use `pytest`:
   ```
   $ uv run pytest
   ```

## Testing

```
$ uv run pytest
```

Tests run against a **local, open-source PySpark session** — no Databricks workspace, no
credentials, no live cluster. `tests/conftest.py` starts a plain `SparkSession` in local
mode. This works because Apache Spark 4.0 open-sourced the `VARIANT` type,
`try_variant_get` and `variant_explode` from Databricks Runtime, and that's all the tested
logic uses. You need a local JDK 17+ on `PATH` (PySpark embeds a JVM); nothing else.

### Three layers, three questions

Each layer answers one question, and the question is the reason the layer exists. They are
independent — a change usually only touches one. Each layer is a folder, and the entity is
in the filename, so every layer has the same shape and adding an entity means adding one
file per layer rather than editing shared ones.

| Layer | Folder and files | Question | What breaks if it's missing |
|---|---|---|---|
| 1. Shaping | `tests/layer1_shaping/` — `test_profile_shaping.py`, `test_talent_shaping.py`, `test_user_shaping.py` | Does the transformation code do what we intended? | A parsing bug ships. Caught by every other test suite in the world; the ordinary one. |
| 2. Schema contract | `tests/layer2_contract/` — `test_profile_contract.py`, `test_talent_contract.py`, `test_user_contract.py` | Does the declared schema still describe what the code produces? | The schema Lakeflow takes is a raw DDL **string**. `py_compile`, `ruff`, `pytest` and `databricks bundle validate` all pass green on one that cannot even parse. It is rendered from `schemas/<entity>.yml` now, so nobody hand-quotes it — but the columns still have to match what the shaping function emits, and only this layer checks that. |
| 3. Data quality checks | `tests/layer3_rules/` — `test_rule_hygiene.py`, plus `test_profile_rules.py`, `test_talent_rules.py`, `test_user_rules.py`, `test_child_rules.py` | Are the quality checks themselves right? | Same problem, worse consequence. DQX resolves a check's columns at apply time and **skips** one it can't resolve rather than failing, so a check naming a column that doesn't exist either quarantines 100% of rows or reports **100% pass forever** — both while the pipeline reports a healthy run. |

`tests/conftest.py` and `tests/helpers.py` stay at the root of `tests/`: they are shared by
all three layers and belong to none of them.

Layer 3 is split two ways rather than three, and the split is the point.
`test_rule_hygiene.py` is *cross-cutting*: it iterates `CHECKS` and checks every check in the
project for well-formedness, snake_case naming, project-wide name uniqueness and
error-criticality policy, plus the two-directional key check that every dataset in `checks/`
is one a transformation applies and vice versa, and that each has a valid view and a
quarantine table. Nobody edits it to add an entity. The `test_*_rules.py` files are the
*behavioural* half: your checks pass clean fixtures and fire on the records built to break
them. Only you can write that half.

**`assert_no_skipped_checks` is the single most important assertion in the suite**, and it
exists because of the row in the table above. `DQEngine.validate_checks` catches an unknown
function, a bad argument name and an invalid criticality without touching Spark — the
`checks.py` loader runs it at import, so those fail a pipeline update and a pytest run
identically. It does *not* catch a column that doesn't exist, because that needs a DataFrame.
`helpers.assert_no_skipped_checks` explodes `_errors`/`_warnings`, looks for `skipped = true`
and fails naming the check, the message and the columns actually available. Every behaviour
test calls it. Without it, a typo'd column name is invisible in every direction.

**It has one limit, and it is worth knowing because it looks like success.** DQX decides to
skip at *analysis* time but reports it *per row*, so a result with no rows carries no
`skipped` marker and there is nothing to find — a typo'd column in a check applied to an
empty DataFrame used to pass clean. The helper now rejects an empty result outright rather
than returning quietly, and `test_assert_no_skipped_checks_rejects_an_empty_result` pins
that. Where a fixture legitimately yields no rows, the test says so explicitly — see
`KNOWN_EMPTY` in `tests/layer3_rules/test_child_rules.py`. The second limit has no guard:
under `ExtraParams(suppress_skipped=True)` DQX emits no marker at all and this assertion
cannot see the skip by any route. It works *because* `conftest.py` leaves that flag off.

Layer 3 is the one people skip, and it's the reason `whoz_ingestion/checks/` exists — see
"Data quality checks" below.

**Why checks and schemas are data, not literals.** Both only a running pipeline ever really
reads, and both fail silently when wrong. A DDL string with one unescaped apostrophe inside a
`COMMENT` ends the literal early and takes the whole schema down; a check naming a column that
doesn't exist enforces nothing. Neither `py_compile` nor `ruff` nor `databricks bundle
validate` looks inside either. Keeping both as YAML a plain-Python loader reads buys three
things: the quoting is done once by code instead of being remembered at every column, the
loader can *validate* at import (so a malformed check raises in a pipeline update and in
pytest identically, rather than yielding a check set that enforces nothing), and — the actual
point — a test can import the same files the pipeline does, with no Databricks workspace, and
run every check against real DataFrame output.

### Fixtures

`fixtures/<entity>/*.json` — one folder per source entity, each file a JSON **array** of
source objects in exactly the shape Whoz exports, so a fixture can be a trimmed copy of real
records without reformatting. The three file names are a convention, and the convention is
the contract:

| File | Contains | Every rule must |
|---|---|---|
| `typical.json` | ordinary, fully-populated records, realistic 24-hex ids | **pass** |
| `hazards.json` | one record per documented type hazard — polymorphic fields, absent vs. null vs. all-null objects, every timestamp precision, near-empty records | **pass** — awkward is not invalid |
| `violations.json` | records designed to trip the quality checks | **fire**, with exactly the expected counts |

Records in `hazards.json`/`violations.json` use descriptive ids (`hazard-absent-headline`,
`violation-profile-as-array`) rather than realistic ObjectIds, so assertions read as the
thing being tested.

Per entity, `conftest.py` gives you a file loader and an inline builder:

```python
def test_something(talent_fixture):        # a stored fixture file
    result = shape_talent(talent_fixture("hazards"))

def test_one_thing(talent_bronze):         # inline, for a pointed case
    result = shape_talent(talent_bronze([{"id": "t1", "profile": {"id": "p1"}}]))
```

Use a file when the records describe the *source* ("this is what Whoz sends"); inline when
they describe the *test* ("one field set, to isolate one behaviour").

**Never build `VARIANT` input by hand.** Use the fixtures. `bronze_of` is the only place in
the suite that constructs a `VARIANT` column, because getting it wrong is easy and the
failure is baffling: round-trip a `VARIANT` value through a local `Row` (via `.first()`, or
by collecting and re-creating) and Spark loses the logical type, re-infers the physical
`STRUCT<metadata: BINARY, value: BINARY>` layout, and `try_variant_get` fails with
`DATATYPE_MISMATCH` even though the JSON is perfectly fine.

**Never assert on a collected timestamp.** Put the DataFrame through
`helpers.to_utc_strings()` first. `conftest.py` pins `spark.sql.session.timeZone` to UTC,
but that governs Spark-side rendering only — `df.collect()` converts `TIMESTAMP`s to Python
datetimes using the **JVM default** zone, which the setting doesn't reach. Measured on this
project: the same value reads `04:04:05` on a laptop in Europe/Madrid and `03:04:05` on
GitHub's UTC runners, so the assertion passes locally and fails in CI.

### Data quality checks

Every check the pipeline enforces lives in `src/whoz_ingestion/checks/<dataset>.yml`, one
file per dataset, as a **native Databricks Labs [DQX](https://databrickslabs.github.io/dqx/)
check list** — the exact format `DQEngine.validate_checks` and `apply_checks_by_metadata`
accept, with no translation layer of our own:

```yaml
- name: profile_id_not_null
  criticality: error
  check:
    function: is_not_null
    arguments:
      column: profile_id

- name: completion_rate_is_fraction
  criticality: warn
  check:
    function: is_in_range
    arguments: { column: completion_rate, min_limit: 0, max_limit: 1 }
```

`checks.py` is a loader over that folder; it exposes one dict, `CHECKS`, keyed by filename
stem, and on import it runs `DQEngine.validate_checks` on every dataset, requires an explicit
`name:` on every check and requires those names to be unique project-wide. The dataset key is
the name of the `@dp.temporary_view` / `@dp.table` function the checks protect, and
`test_rule_hygiene.py` checks that correspondence both ways — a key no dataset defines, or a
dataset whose checks nobody applies, fails locally.

Prefer a **built-in check function** to `sql_expression` wherever one fits: built-ins carry
typed messages and tested edge handling, and `is_in_range` in particular is null-safe by
construction, so the `x IS NULL OR …` wrapper these predicates used to need is gone. Reach for
`sql_expression` only for things no built-in covers — two-column comparisons, parse checks —
and give it a `msg:` that explains what it means that the check fired, because DQX has no
typed message to fall back on there.

**Criticality is a contract, not a label:**

* `error` → the row is **withheld**: excluded from the silver table, written to the dataset's
  quarantine table instead. Only for a row that is unusable — in practice, a null primary key.
  `test_error_criticality_checks_only_ever_guard_a_key` enforces that by inspecting the check
  *function*, because withholding rows is invisible: the pipeline reports a healthy run and
  the data is simply not there.
* `warn` → the row is in **both**: it flows into silver as normal *and* is written to
  quarantine carrying the `_warnings` struct that says why. Everything else: assumptions about
  the source that hold today and should raise an eyebrow, not remove data, if they ever stop.

### How a checked dataset is wired

Every dataset with checks is three objects in `transformations/silver/`, the documented DQX
pattern:

```python
dq = DQEngine(WorkspaceClient(), spark=spark)      # once per module

@dp.temporary_view                                  # 1. apply the checks, once
def whoz_profile_checked():
    return dq.apply_checks_by_metadata(shape_profile(...), CHECKS["whoz_profile_shaped"])

@dp.temporary_view                                  # 2. the rows that pass
def whoz_profile_shaped():                          #    name unchanged -> AUTO CDC untouched
    return dq.get_valid(spark.readStream.table("whoz_profile_checked"))

@dp.table(name=..., table_properties={"quality": "quarantine"})   # 3. the rows that didn't
def whoz_profiles_quarantine():
    return dq.get_invalid(spark.readStream.table("whoz_profile_checked"))
```

`get_valid` returns the base columns **without** `_errors`/`_warnings`, which is why the AUTO
CDC flows and the declared schemas did not change at all. `whoz_profile_skill_ratings` and
`whoz_profile_completion_rules` have no checks and so have neither of the other two objects.

**Querying quarantine.** A quarantine table is the base columns plus two arrays of result
structs (`name`, `message`, `columns`, `function`, `run_time`, `skipped`, …). It is **not** a
dead-letter queue — `warn` rows are in it *and* in silver.

```sql
-- what was actually withheld from silver
SELECT * FROM silver.whoz_profiles_quarantine WHERE _errors IS NOT NULL;

-- which checks are firing, and how often
SELECT r.name, r.message, count(*) AS rows
FROM silver.whoz_profiles_quarantine
LATERAL VIEW explode(concat(coalesce(_errors, array()), coalesce(_warnings, array()))) AS r
GROUP BY r.name, r.message ORDER BY rows DESC;

-- the alarm: a check DQX could not resolve, so it checked nothing (or everything)
SELECT * FROM silver.whoz_profiles_quarantine
LATERAL VIEW explode(concat(coalesce(_errors, array()), coalesce(_warnings, array()))) AS r
WHERE r.skipped;
```

**To add a check:** add one entry to the dataset's `checks/<dataset>.yml`, and one record to
`fixtures/<entity>/violations.json` that trips it. The hygiene tests pick the check up
automatically; `test_every_<entity>_rule_is_covered_by_the_violations_fixture` fails until the
fixture record exists, so a check can't be added without evidence that it works.

Table schemas work the same way and for the same reasons — `schemas/<entity>.yml` plus
`contract.py`, which renders the columns into the DDL string `create_streaming_table` takes
and does the apostrophe escaping once, in code.

**On DQX — adopted at runtime, with two consequences worth knowing.** Databricks Labs'
[DQX](https://databrickslabs.github.io/dqx/) now evaluates every quality check this pipeline
enforces, at runtime, with a quarantine table per checked dataset. It replaced
`@dp.expect_all` / `@dp.expect_all_or_drop` entirely; there are no expectation decorators left
in `transformations/`. What that bought is the quarantine tables: a `warn` row used to be a
number on a dashboard, and is now a row you can query, join and count by check name.

Two things it cost, both accepted deliberately:

* **The pipeline graph's Data Quality tab no longer populates for these datasets.** DQX's
  Lakeflow integration does not use Expectations — it uses its own methods — and Databricks
  documents that a query with no expectations defined has no data quality metrics. The
  quarantine tables are now the only place a `warn` finding is visible. That is a richer place
  than the tab was, but it is a place you have to go to rather than one that greets you.
* **`databricks-labs-dqx` is a pipeline runtime dependency.** It is classified Beta on PyPI
  and ships an explicit no-SLA / AS-IS disclaimer, with roughly one breaking change per minor
  release, and it is now on the ingestion path of every silver table. `pyproject.toml` pins it
  to a compatible-minor range for that reason. `pandas` is pinned alongside it because DQX
  imports pandas at module scope without declaring it as a dependency — without that line,
  `from databricks.labs.dqx.engine import DQEngine` raises `ModuleNotFoundError`.

**And one failure mode the old decorators did not have.** DQX resolves a check's columns at
apply time. When it cannot — a typo'd `column:`, an `sql_expression` naming something that
isn't there — it does not raise. It marks the check `skipped=true` and emits that on every
row: at `error` criticality the silver table empties into quarantine, and under
`suppress_skipped` the check silently becomes a no-op. `@dp.expect_all` failed the update
loudly instead. `helpers.assert_no_skipped_checks` is what closes that hole, and it is called
by every behaviour test in `tests/layer3_rules/` — including the ones that assert checks *do*
fire, since a skipped check is not a fired one. Mutating a column name in a `checks/*.yml`
fails locally with the bad column named — how many tests depends on how many fixtures reach
that dataset with rows, and for the child datasets it is one, not four. (Measured: a typo in
`whoz_position_aptitude_refs.yml` fails only the `typical` case, because the `hazards` fixture
produces no rows for it at all.) None of this is an alternative to layers 1 and 2 — DQX
validates *data* at runtime, pytest validates *code* before merge.

### Adding a source entity

The unit of extension is the **entity**, and the seam is `src/whoz_ingestion/`. Everything generic
comes free: `tests/helpers.py` is entity-agnostic, and the hygiene tests in
`tests/layer3_rules/test_rule_hygiene.py` iterate `CHECKS`, so adding a
`checks/<dataset>.yml` is enough to have every one of its checks validated by DQX and checked
for snake_case naming, project-wide name uniqueness and error-criticality policy.
`whoz_talents` was added this way; copy it — literally, since the per-entity test files are
one per layer and there is no shared test file to append to.

**[`docs/adding_a_source_entity.md`](docs/adding_a_source_entity.md) is the checklist** —
thirteen numbered steps, what each file must contain, which test catches each mistake, and the
two things nothing catches. Follow it there rather than from here: this section used to carry
a seven-step summary of the same work, the two lists were renumbered independently, and a
summary that disagrees with the checklist about how many steps there are is worse than no
summary. Roughly: fixtures, then `conftest.py`, then the shaping and checks files, then one
test per layer, then the thin transformation wrappers and the pipeline config.

The talent entity is worth reading as the worked example, particularly
`whoz_ingestion/shaping/talent.py`'s header: it documents *what it deliberately does not model*
(the nested profile object) and why, which is the kind of decision that is invisible six
months later.

### Why the transformations aren't tested directly

`pyspark.pipelines` (imported as `dp` in every file under `transformations/`) is only fully
present inside a running Lakeflow pipeline, and the module-level `spark.conf.get` calls fail
with no pipeline configuration. Those files also construct a `DQEngine(WorkspaceClient())` at
import, which needs workspace credentials. So the row-shaping logic lives in
`src/whoz_ingestion/`, which has no `pipelines` dependency at all and takes/returns plain
DataFrames. Each `@dp.table` function is a thin wrapper: read the upstream dataset, call the
`whoz_ingestion` function or a DQX method, return the result. Test the `whoz_ingestion`
function; don't try to call the `@dp.table` one.

### What's covered today, and what isn't

Every check in the project is applied to real DataFrame output by the same `DQEngine` call the
pipeline makes, so a typo'd column name fails locally rather than being silently skipped at
pipeline update:

* `shape_profile()` → `silver.whoz_profiles`, `silver.whoz_profile_history`
* `shape_talent()` → `silver.whoz_talents`, `silver.whoz_talent_versions`
* `shape_user()` → `silver.whoz_users`, `silver.whoz_user_versions`
* the child datasets — `whoz_profile_aptitudes`, `whoz_profile_positions`,
  `whoz_position_aptitude_refs`, `whoz_talent_workspace_history` — via their
  `whoz_ingestion/shaping/<table>.py` modules, whose queries
  `tests/layer3_rules/test_child_rules.py` runs against a bronze fixture. This was the last
  blind spot: those ten checks, half of the project's total, used to be parsed but never
  resolved.

`whoz_profile_skill_ratings` and `whoz_profile_completion_rules` have no checks, so there is
nothing for layer 3 to check on them and neither has a quarantine table; their queries live in
`shaping/` with the rest and can be checked the same way the day they get one.

Still only partly covered is the **`violations.json` half for the child datasets**. The
`user` entity closed it for `whoz_user_workspace_roles` — all three of its checks are made to
fire by `fixtures/whoz_users/violations.json`, and
`test_every_user_child_rule_is_covered_by_the_violations_fixture` keeps it that way. The four
profile and talent child datasets still have no violating record, so ten checks are applied
but never fired. `test_user_child_rules_catch_the_records_designed_to_break_them` is the shape
the others should grow into.

Separately, `hazard-extended-year-end-date` looks like it should
trip `end_date_parsed` — that check was written for it — and does not: Spark 4.0 parses
`+22015-07-31` as a valid `DATE` rather than returning NULL, contradicting
`docs/whoz_profile_data_model.md` §2. `test_extended_year_end_date_parses_rather_than_nulling`
pins that, so the day the behaviour changes (or Databricks Runtime turns out to differ) it
is a visible failure and not a surprise.

Also uncovered, and it can only be covered by deploying: **the pipeline wiring itself**. That
the `_checked` views resolve, that `spark.readStream.table("<view>")` attaches to a temporary
view in the pipeline graph, that `WorkspaceClient()` authenticates on serverless, and that the
quarantine tables infer a usable schema — none of that runs in pytest, and `databricks bundle
validate` does not reach it either. Count the datasets in the first update's graph.

## CI/CD

Three environment branches — `dev`, `test`, `main` — drive two workflows. A **PR into**
one of them runs pytest and validates the matching bundle target; a **push to** one of them
deploys that target. (`local` has no branch; it's never touched by CI, see Environments
above.)

Full detail — job breakdown, service principal setup, secret rotation, failure triage —
is in [`.github/workflows/README.md`](../.github/workflows/README.md).
Current configuration state and known gaps are in [`docs/cicd.md`](../docs/cicd.md).

# oxygen_medallion_pipeline

Databricks asset bundle for Project Oxygen's medallion pipelines. One bundle, multiple
sources: each source is a self-contained pipeline + refresh job under `resources/` and
`src/`, sharing this bundle's targets, catalog variables, permissions, and CI/CD.

Sources today:

* **Whoz** — two export files ingested by one pipeline: **profile** and **talent**. See
  "The Whoz source" below, which has a section per entity.

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
    test suite — `shaping/<entity>.py` (the pure DataFrame-in/DataFrame-out row logic
    plus each table's DDL constant) and `expectations.py` (the data quality rules as
    data). Nothing here imports `pyspark.pipelines`, which is what keeps it testable;
    see Testing below.

    A sibling of the pipeline folder rather than a subfolder of it, because it has two
    consumers and belongs to neither. It resolves under the same name in both: the
    pipeline's `root_path` is `src` and `pyproject.toml`'s `pythonpath` names that same
    folder, so `whoz_ingestion.shaping.profile` is one import path in a running pipeline
    and in pytest alike — which is what makes a wrong prefix fail locally instead of at
    deploy time. Keep those two settings in step.
* `resources/`:  Resource configurations (jobs, pipelines, etc.), one pair per source.
* `docs/`: Source data model analysis (`docs/whoz_profile_data_model.md`) and the
  step-by-step runbook for ingesting a new export (`docs/adding_a_source_entity.md`).
* `tests/`: The test suite — one folder per layer (`layer1_shaping/`, `layer2_contract/`,
  `layer3_rules/`), entity in the filename. See Testing below.
* `fixtures/`: Sample source records the tests run against, one folder per source entity.

## The Whoz source

Whoz exports several files. Each is its own **entity**: its own Auto Loader stream, its
own schema location, its own bronze and silver tables, its own fixtures and tests. One
pipeline, `whoz_ingestion_etl`, models all of them — they share the pipeline, not their
tables. Two today, each with its own section below; a third would get a third section
rather than extra rows in an existing table.

| Entity | Export | Section |
|---|---|---|
| profile | the profile report | [Profile](#profile) |
| talent | the talent report (carries a nested profile object) | [Talent](#talent) |

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

Code: `transformations/bronze/whoz_profiles.py`, `transformations/silver/whoz_profile.py`
and `transformations/silver/whoz_profile_children.py` (the four child tables), over
`whoz_ingestion/shaping/profile.py`. Landing config keys:
`whoz.profiles.source_path`, `whoz.profiles.schema_path`.

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

Code: `transformations/bronze/whoz_talents.py` and
`transformations/silver/whoz_talent.py`, over `whoz_ingestion/shaping/talent.py`.
Landing config keys: `whoz.talents.source_path`, `whoz.talents.schema_path`.

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

> **Known open bug.** Those four paths are hardcoded to `/Volumes/oxygen_dev/...` instead
> of being `${var.catalog}`-qualified, so a `test` or `prod` deploy reads *dev's* landing
> volume. It has not bitten yet because only dev has been run against real files. Fix all
> four together when you fix one.


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
| 1. Shaping | `tests/layer1_shaping/` — `test_profile_shaping.py`, `test_talent_shaping.py` | Does the transformation code do what we intended? | A parsing bug ships. Caught by every other test suite in the world; the ordinary one. |
| 2. Schema contract | `tests/layer2_contract/` — `test_profile_contract.py`, `test_talent_contract.py` | Does the declared schema still describe what the code produces? | `PROFILE_COLUMNS` is a raw DDL **string**. `py_compile`, `ruff`, `pytest` and `databricks bundle validate` all pass green on a schema that cannot even parse — one unescaped apostrophe in a `COMMENT` does it. Only a live pipeline update would notice. |
| 3. Data quality rules | `tests/layer3_rules/` — `test_rule_hygiene.py`, plus `test_profile_rules.py`, `test_talent_rules.py` | Are the quality rules themselves right? | Same problem, worse consequence. A `@dp.expect` predicate is a string too, so a rule naming a column that doesn't exist, or one that catches nothing, reports **100% pass forever** and nobody looks again. |

`tests/conftest.py` and `tests/helpers.py` stay at the root of `tests/`: they are shared by
all three layers and belong to none of them.

Layer 3 is split two ways rather than three, and the split is the point.
`test_rule_hygiene.py` is *cross-cutting*: it iterates `ALL_RULE_SETS` and checks every
rule in the project for parseability, snake_case naming, project-wide name uniqueness and
drop-rule policy. Nobody edits it to add an entity — registering your rule sets is what
makes it cover them. The per-entity `test_*_rules.py` files are the *behavioural* half:
your rules pass clean fixtures and fire on the records built to break them. Only you can
write that half.

Layer 3 is the one people skip, and it's the reason `whoz_ingestion/expectations.py` exists —
see "Data quality rules" below.

### Fixtures

`fixtures/<entity>/*.json` — one folder per source entity, each file a JSON **array** of
source objects in exactly the shape Whoz exports, so a fixture can be a trimmed copy of real
records without reformatting. The three file names are a convention, and the convention is
the contract:

| File | Contains | Every rule must |
|---|---|---|
| `typical.json` | ordinary, fully-populated records, realistic 24-hex ids | **pass** |
| `hazards.json` | one record per documented type hazard — polymorphic fields, absent vs. null vs. all-null objects, every timestamp precision, near-empty records | **pass** — awkward is not invalid |
| `violations.json` | records designed to trip the quality rules | **fire**, with exactly the expected counts |

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

### Data quality rules

Every rule the pipeline enforces lives in `src/whoz_ingestion/expectations.py`
as a `{name: SQL predicate}` dict, not as a decorator argument. Two things read those dicts:
the pipeline, via `@dp.expect_all` / `@dp.expect_all_or_drop`, and the test suite. That
indirection is the whole point — the module imports nothing, so a test can import it
without a Spark session and evaluate every predicate against real DataFrame output.

The suffix is a contract, not a label:

* `*_MUST_HOLD` → `@dp.expect_all_or_drop`. Violating rows are **dropped**. Only for a row
  that is unusable — in practice, a null primary key. `test_drop_rules_only_ever_guard_a_key`
  enforces this, because dropping rows is invisible: the pipeline reports a healthy run and
  the data is simply not there.
* `*_SHOULD_HOLD` → `@dp.expect_all`. Violating rows are **kept and counted**. Everything
  else: assumptions about the source that hold today and should raise an eyebrow, not delete
  data, if they ever stop.

**To add a rule:** add one line to the right dict, and one record to
`fixtures/whoz_profiles/violations.json` that trips it. The hygiene tests pick the rule up
automatically; `test_every_profile_rule_is_covered_by_the_violations_fixture` fails until the
fixture record exists, so a rule can't be added without evidence that it works.

**On DQX.** Databricks Labs' [DQX](https://databrickslabs.github.io/dqx/) solves the same
problem as this layer, with a quarantine-table pattern and a data-health dashboard on top.
It's worth a look if quality reporting grows beyond what Lakeflow's built-in expectation
metrics give you. It is not an alternative to layers 1 and 2 — DQX validates *data* at
runtime, pytest validates *code* before merge. The rules-as-data shape here is deliberately
close to DQX's own, so moving is additive rather than a rewrite.

### Adding a source entity

The unit of extension is the **entity**, and the seam is `src/whoz_ingestion/`. Everything generic
comes free: `tests/helpers.py` is entity-agnostic, and the hygiene tests in
`tests/layer3_rules/test_rule_hygiene.py` iterate `ALL_RULE_SETS`, so registering your rules
is enough to have every one of them checked for parseability, snake_case naming,
project-wide name uniqueness and drop-rule policy. `whoz_talents` was added this way; copy
it — literally, since the per-entity test files are one per layer and there is no shared
test file to append to.

The seven headline steps are below. **`docs/adding_a_source_entity.md` is the full
checklist** — what each file must contain, which test catches each mistake, and the two
things nothing catches.

1. **`fixtures/<entity>/{typical,hazards,violations}.json`** — real records, trimmed.
2. **`conftest.py`** — one entry in `BRONZE_KEYS` (the identity columns and their JSON
   paths), one in `SOURCE_FILES`, and two one-line fixtures at the bottom.
3. **`whoz_ingestion/shaping/<entity>.py`** — `<ENTITY>_COLUMNS` (DDL), `<ENTITY>_HISTORY_COLUMNS`
   if it gets an SCD2 table, and `shape_<entity>(bronze) -> DataFrame`. No `pyspark.pipelines`
   import, ever — that's what keeps it testable.
4. **`whoz_ingestion/expectations.py`** — `<ENTITY>_MUST_HOLD` / `<ENTITY>_SHOULD_HOLD`, both
   registered in `ALL_RULE_SETS`. Rule names must be unique project-wide. The one shared
   file you still edit, and deliberately so — see the doc for why.
5. **`tests/layer1_shaping/test_<entity>_shaping.py`** — one test per hazard.
6. **`tests/layer2_contract/test_<entity>_contract.py`** (three tests) and
   **`tests/layer3_rules/test_<entity>_rules.py`** (three tests) — both **new files**,
   copied from the talent pair. `test_rule_hygiene.py` you never touch.
7. **`transformations/bronze/<entity>.py` + `silver/<entity>.py`** — thin wrappers, plus the
   pipeline `configuration` entries for the source path and schema location. The bronze
   file's `try_variant_get` key paths must match `BRONZE_KEYS`, or the tests build input by
   a different route than production reads it.

The talent entity is worth reading as the worked example, particularly
`whoz_ingestion/shaping/talent.py`'s header: it documents *what it deliberately does not model*
(the nested profile object) and why, which is the kind of decision that is invisible six
months later.

### Why the transformations aren't tested directly

`pyspark.pipelines` (imported as `dp` in every file under `transformations/`) is only fully
present inside a running Lakeflow pipeline: the expectation decorators don't exist in
open-source PySpark, and the module-level `spark.conf.get` calls fail with no pipeline
configuration. So the row-shaping logic lives in `src/whoz_ingestion/`, which has no
`pipelines` dependency at all and takes/returns plain DataFrames. Each `@dp.table` function
is a thin wrapper: read the upstream table, call the `whoz_ingestion` function, return the
result. Test the `whoz_ingestion` function; don't try to call the `@dp.table` one.

### What's covered today, and the next step

Covered, at all three layers, resolved against real DataFrame output so a typo'd column name
in a rule fails locally:

* `shape_profile()` → `silver.whoz_profiles`, `silver.whoz_profile_history`
* `shape_talent()` → `silver.whoz_talents`, `silver.whoz_talent_versions`

Not yet: the child tables (`whoz_profile_aptitudes`, `whoz_profile_positions`,
`whoz_position_aptitude_refs`, `whoz_profile_skill_ratings`, `whoz_profile_completion_rules`,
`whoz_talent_workspace_history`). Their `spark.sql(...)` bodies still sit inline in
`transformations/silver/`, so their rules get hygiene checks only — a rule naming a column
that doesn't exist on those tables would still slip through. Pulling those bodies into
`whoz_ingestion/shaping/` the way `shaping/profile.py` and `shaping/talent.py` were done is the
one change that closes it; the tests to add afterwards are already sketched in
`tests/layer3_rules/test_rule_hygiene.py`'s header.

## CI/CD

Three environment branches — `dev`, `test`, `main` — drive two workflows. (`local` has
no branch; it's never touched by CI, see Environments above.)

| Branch | Bundle target | PR into it (`.github/workflows/databricks-ci.yml`) | Push to it (`.github/workflows/databricks-cd.yml`) |
|---|---|---|---|
| `dev` | `dev` | validates the `dev` target + runs pytest | deploys `dev` |
| `test` | `test` | validates the `test` target + runs pytest | deploys `test` |
| `main` | `prod` | validates the `prod` target + runs pytest | deploys `prod`, gated by the `prod` GitHub Environment |

`databricks bundle validate`/`deploy` need a live authenticated CLI session against the
target workspace regardless of target, so both authenticate as the `sp-oxygen-cicd`
service principal over OAuth M2M, via three repo secrets: `DATABRICKS_HOST`,
`DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`. `unit-tests` needs none of these —
see Testing above.

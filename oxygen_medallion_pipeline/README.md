# oxygen_medallion_pipeline

Databricks asset bundle for Project Oxygen's medallion pipelines. One bundle, multiple
sources: each source is a self-contained pipeline + refresh job under `resources/` and
`src/`, sharing this bundle's targets, catalog variables, permissions, and CI/CD.

Sources today:

* **Whoz** — profile export ingestion. See "The Whoz profile pipeline" below.

To add a new source, add `resources/<source>.pipeline.yml` + `resources/<source>.job.yml`
(picked up automatically by `databricks.yml`'s `include: resources/*.yml`) and a
`src/<source>_etl/` folder alongside `whoz_ingestion_etl/`. No new bundle, no new
`databricks.yml`.

* `src/`: Python source code for this project.
  * `src/whoz_ingestion/`: Shared Python code for the Whoz source, used by its jobs/pipelines.
  * `src/whoz_ingestion_etl/transformations/`: the `@dp.table`-decorated bronze/silver
    dataset definitions of the `whoz_ingestion_etl` pipeline.
  * `src/whoz_ingestion_etl/utilities/`: the pure DataFrame-in/DataFrame-out shaping
    logic those datasets call — kept separate so it's unit-testable, see Testing below.
* `resources/`:  Resource configurations (jobs, pipelines, etc.), one pair per source.
* `docs/`: Source data model analysis — see `docs/whoz_profile_data_model.md`.
* `tests/`: Unit tests for the shared Python code.
* `fixtures/`: Fixtures for data sets (primarily used for testing).

## The Whoz profile pipeline

`whoz_ingestion_etl` lands the Whoz profile export and models it:

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

`bronze`/`silver` schema names are literal and identical in **every** target — the
isolation boundary is the catalog, not the schema (see "Environments" below). Every
table name in `transformations/*.py` is built from `CATALOG`/`BRONZE_SCHEMA`/
`SILVER_SCHEMA` module-level constants (read from pipeline config via `spark.conf.get`),
never hardcoded.

The export is a pretty-printed JSON **array**, not JSONL, and it is polymorphic in
several fields, so bronze reads it with `multiLine` + `singleVariantColumn` and silver
casts lazily with `try_variant_get`. `docs/whoz_profile_data_model.md` explains why.

The landing folder is set in the pipeline's `configuration` block in
`resources/whoz_ingestion_etl.pipeline.yml` (`whoz.profiles.source_path` and
`whoz.profiles.schema_path`) — currently the shared `oxygen_dev.landing.source` volume,
filtered to Whoz's files by filename glob.


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

Tests run against a **local, open-source PySpark session** — no Databricks workspace,
no credentials, no live cluster. `tests/conftest.py` starts a plain `SparkSession` in
local mode. This works because Apache Spark 4.0 open-sourced the `VARIANT` type,
`try_variant_get` and `variant_explode` from Databricks Runtime, and that's all the
tested logic uses. You need a local JDK 17+ on `PATH` (PySpark embeds a JVM); nothing
else.

**Why the transformations aren't tested directly.** `pyspark.pipelines` (imported as
`dp` in every file under `transformations/`) only exists inside a running Lakeflow
pipeline — importing a `transformations/*.py` file from a plain pytest process raises
`ImportError: cannot import name 'pipelines' from 'pyspark'`. So the actual row-shaping
logic (the `.select(...)` / `try_variant_get` calls) lives in `utilities/`, which has no
`pipelines` dependency at all and takes/returns plain DataFrames. Each `@dp.table`
function in `transformations/` is a thin wrapper: read the upstream table, call the
`utilities` function, return the result. Test the `utilities` function; don't try to
call the `@dp.table` function.

**How to build test input.** Build the whole DataFrame in one
`spark.createDataFrame(...).select(...)` call — see `_bronze_df` in
`tests/test_silver_whoz_profile.py`. Do not build rows one at a time via `.first()` and
reassemble them into a list of `Row`s: round-tripping a `VARIANT` value through a local
`Row` loses its type, and Spark re-infers it as the physical
`STRUCT<metadata: BINARY, value: BINARY>` layout instead — `try_variant_get` then fails
with `DATATYPE_MISMATCH` even though the JSON is fine.

**What's covered vs. not, today.** `test_silver_whoz_profile.py` covers `shape_profile()`
(the row-shaping logic behind `silver.whoz_profiles`/`silver.whoz_profile_history`) only,
against the type hazards documented in `docs/whoz_profile_data_model.md`
(int/float `completionRate`, absent-vs-null `headline`). The other tables in
`transformations/` don't have a `utilities` counterpart yet — extending the pattern
(pulling their `.select(...)` / `spark.sql(...)` bodies into `utilities/`, one module per
table) is the natural next step whenever they need a test.

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

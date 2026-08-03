# whoz_ingestion_etl

The Lakeflow pipeline for the Whoz source. This folder holds **only** what Lakeflow
loads and executes:

- `transformations/`: all dataset definitions, split layer-first into `bronze/` and
  `silver/`. Layer-first rather than entity-first because a future `gold/` would join
  across entities and needs a peer folder to live in.

Everything these files *call* lives one level up, in `../whoz_ingestion/` — the shared
package, which imports no `pyspark.pipelines` and is therefore unit-testable:
`shaping/<entity>.py` (DataFrame-in/DataFrame-out row logic plus each table's DDL
constant) and `expectations.py` (the data quality rules as data). The tests import those
same modules. The import prefix is `whoz_ingestion.x` in both places, because the
pipeline's `root_path` is `src/` rather than this folder — see `../whoz_ingestion/__init__.py`.

## Getting Started

Most of the relevant source code lives under `transformations/`, one entity at a time.

### Profile — the Whoz profile export

* `bronze/whoz_profiles.py` — Auto Loader ingestion into `bronze.whoz_profiles` (whole
  JSON object kept in one VARIANT column), plus the
  `bronze.whoz_profiles_payload_shapes` drift monitor.
* `silver/whoz_profile.py` — `silver.whoz_profiles` (current state, AUTO CDC SCD1),
  `silver.whoz_profile_history` (full version history, AUTO CDC SCD2) and
  `silver.whoz_profile_completion_rules` (the completionDetails map, exploded).
* `silver/whoz_profile_children.py` — `silver.whoz_profile_aptitudes`,
  `silver.whoz_profile_positions`, `silver.whoz_position_aptitude_refs` and the legacy
  `silver.whoz_profile_skill_ratings`.

### Talent — the Whoz talent export

A separate export with its own file, its own Auto Loader stream and its own schema
location; it is not a variant of the profile one.

* `bronze/whoz_talents.py` — ingestion into `bronze.whoz_talents` (payload includes the
  nested `profile` object, untouched), plus the `bronze.whoz_talents_payload_shapes`
  drift monitor, which groups by top-level key set *and* by the container type of that
  nested profile.
* `silver/whoz_talent.py` — `silver.whoz_talents` (current state, AUTO CDC SCD1),
  `silver.whoz_talent_versions` (full version history, AUTO CDC SCD2) and
  `silver.whoz_talent_workspace_history` (the source's own `history[]` array, exploded).
  The two "history" names are not interchangeable: `whoz_talent_versions` is how the
  talent *record* changed over time, `whoz_talent_workspace_history` is which workspaces
  the talent has belonged to. The embedded `profile` object is deliberately not
  re-modelled here — `silver.whoz_profiles` already does that, and this table carries
  `profile_id` as a foreign key into it.

Bronze and silver are their own schemas, literally named `bronze` and `silver`, in
**every** target — including your personal sandbox. What changes between environments is
the catalog, not the schema: the catalog is the isolation boundary. See the
`CATALOG`/`BRONZE_SCHEMA`/`SILVER_SCHEMA` constants at the top of each file, and
`bronze_schema`/`silver_schema` in `../../databricks.yml`, which are `bronze`/`silver` in
all four targets.

The source data model, the field inventory and the type hazards these choices work
around are documented in `../../docs/whoz_profile_data_model.md`. Read it before
changing any of the casts.

* By convention, every dataset under `transformations` is in a separate file.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* If you're using the workspace UI, use `Run file` to run and preview a single transformation.
* If you're using the CLI, use `databricks bundle run whoz_ingestion_etl --select whoz_profiles` to run a single transformation.

For more tutorials and reference material, see https://docs.databricks.com/dlt.

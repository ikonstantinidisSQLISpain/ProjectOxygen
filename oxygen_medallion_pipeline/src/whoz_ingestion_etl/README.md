# whoz_ingestion

This folder defines all source code for the whoz_ingestion pipeline:

- `transformations/`: All dataset definitions, split layer-first into `bronze/` and
  `silver/`. Layer-first rather than entity-first because a future `gold/` would join
  across entities and needs a peer folder to live in.
- `utilities/`: Everything the dataset definitions call that does *not* import
  `pyspark.pipelines`, and is therefore unit-testable — `shaping/<entity>.py` (the
  DataFrame-in/DataFrame-out row logic plus each table's DDL constant) and
  `expectations.py` (the data quality rules as data).

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* `bronze/whoz_profiles.py` — Auto Loader ingestion of the Whoz profile export into
  `bronze.whoz_profiles` (whole JSON object kept in one VARIANT column), plus the
  `bronze.whoz_profiles_payload_shapes` drift monitor.
* `bronze/whoz_talents.py` — the same treatment for the Whoz talent export into
  `bronze.whoz_talents` (payload includes the nested `profile` object, untouched), plus
  the `bronze.whoz_talents_payload_shapes` drift monitor, which groups by top-level key
  set *and* by the container type of that nested profile.
* `silver/whoz_profile.py` — `silver.whoz_profiles` (current state, AUTO CDC SCD1),
  `silver.whoz_profile_history` (full version history, AUTO CDC SCD2) and
  `silver.whoz_profile_completion_rules` (the completionDetails map, exploded).
* `silver/whoz_profile_children.py` — `silver.whoz_profile_aptitudes`,
  `silver.whoz_profile_positions`, `silver.whoz_position_aptitude_refs` and the legacy
  `silver.whoz_profile_skill_ratings`.
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

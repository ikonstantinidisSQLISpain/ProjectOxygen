# whoz_ingestion

This folder defines all source code for the whoz_ingestion pipeline:

- `explorations/`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations/`: All dataset definitions and transformations.
- `utilities/` (optional): Utility functions and Python modules used in this pipeline.
- `data_sources/` (optional): View definitions describing the source data for this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* `bronze_whoz_profiles.py` — Auto Loader ingestion of the Whoz profile export into
  `bronze.whoz_profiles` (whole JSON object kept in one VARIANT column), plus the
  `bronze.whoz_profiles_payload_shapes` drift monitor.
* `silver_whoz_profile.py` — `silver.whoz_profiles` (current state, AUTO CDC SCD1),
  `silver.whoz_profile_history` (full version history, AUTO CDC SCD2) and
  `silver.whoz_profile_completion_rules` (the completionDetails map, exploded).
* `silver_whoz_profile_children.py` — `silver.whoz_profile_aptitudes`,
  `silver.whoz_profile_positions`, `silver.whoz_position_aptitude_refs` and the legacy
  `silver.whoz_profile_skill_ratings`.

Bronze and silver share a schema in dev (your personal sandbox) and split into real
`bronze`/`silver` schemas in prod — see the `CATALOG`/`BRONZE_SCHEMA`/`SILVER_SCHEMA`
constants at the top of each file and `bronze_schema`/`silver_schema` in
`../../databricks.yml`.

The source data model, the field inventory and the type hazards these choices work
around are documented in `../../docs/whoz_profile_data_model.md`. Read it before
changing any of the casts.

* By convention, every dataset under `transformations` is in a separate file.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* If you're using the workspace UI, use `Run file` to run and preview a single transformation.
* If you're using the CLI, use `databricks bundle run whoz_ingestion_etl --select whoz_profiles` to run a single transformation.

For more tutorials and reference material, see https://docs.databricks.com/dlt.

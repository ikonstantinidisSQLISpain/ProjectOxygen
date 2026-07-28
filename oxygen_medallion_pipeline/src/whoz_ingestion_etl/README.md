# whoz_ingestion

This folder defines all source code for the whoz_ingestion pipeline:

- `explorations/`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations/`: All dataset definitions and transformations.
- `utilities/` (optional): Utility functions and Python modules used in this pipeline.
- `data_sources/` (optional): View definitions describing the source data for this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* `bronze_whoz_profiles.py` — Auto Loader ingestion of the Whoz profile export into
  `whoz_profiles_bronze` (whole JSON object kept in one VARIANT column), plus the
  `whoz_profiles_payload_shapes` drift monitor.
* `silver_whoz_profile.py` — `whoz_profile` (one row per profile) and
  `whoz_profile_completion_rule` (the completionDetails map, exploded).
* `silver_whoz_profile_children.py` — `whoz_profile_aptitude`, `whoz_profile_position`,
  `whoz_position_aptitude_ref` and the legacy `whoz_profile_skill_rating`.

The source data model, the field inventory and the type hazards these choices work
around are documented in `../../docs/whoz_profile_data_model.md`. Read it before
changing any of the casts.

* By convention, every dataset under `transformations` is in a separate file.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* If you're using the workspace UI, use `Run file` to run and preview a single transformation.
* If you're using the CLI, use `databricks bundle run whoz_ingestion_etl --select whoz_profile` to run a single transformation.

For more tutorials and reference material, see https://docs.databricks.com/dlt.

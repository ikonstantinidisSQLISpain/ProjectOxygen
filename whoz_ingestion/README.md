# whoz_ingestion

Databricks asset bundle that ingests the Whoz profile export into Unity Catalog.

* `src/`: Python source code for this project.
  * `src/whoz_ingestion/`: Shared Python code that can be used by jobs and pipelines.
  * `src/whoz_ingestion_etl/transformations/`: the bronze/silver datasets of the
    `whoz_ingestion_etl` pipeline.
* `resources/`:  Resource configurations (jobs, pipelines, etc.)
* `docs/`: Source data model analysis — see `docs/whoz_profile_data_model.md`.
* `tests/`: Unit tests for the shared Python code.
* `fixtures/`: Fixtures for data sets (primarily used for testing).

## The Whoz profile pipeline

`whoz_ingestion_etl` lands the Whoz profile export and models it:

| Layer  | Table | Grain |
|---|---|---|
| bronze | `whoz_profiles_bronze` | one row per profile, full JSON in a VARIANT `payload` |
| bronze | `whoz_profiles_payload_shapes` | one row per distinct top-level key set (drift monitor) |
| silver | `whoz_profile` | one row per profile |
| silver | `whoz_profile_completion_rule` | (profile, completion rule) |
| silver | `whoz_profile_aptitude` | one row per declared skill |
| silver | `whoz_profile_position` | one row per job/mission |
| silver | `whoz_position_aptitude_ref` | (position, aptitude) bridge |
| silver | `whoz_profile_skill_rating` | legacy `skillRatings` — verify before use |

The export is a pretty-printed JSON **array**, not JSONL, and it is polymorphic in
several fields, so bronze reads it with `multiLine` + `singleVariantColumn` and silver
casts lazily with `try_variant_get`. `docs/whoz_profile_data_model.md` explains why.

The landing folder is set in the pipeline's `configuration` block in
`resources/whoz_ingestion_etl.pipeline.yml` (`whoz.profiles.source_path` and
`whoz.profiles.schema_path`) — point it at the volume holding the export before
deploying.


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
*  Run `uv sync --dev` to install the project's dependencies.


# Using this project using the CLI

The Databricks workspace and IDE extensions provide a graphical interface for working
with this project. It's also possible to interact with it directly using the CLI:

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

2. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project, including a pipeline
    called `[dev yourname] whoz_ingestion_etl`.
    You can find that resource by opening your workpace and clicking on **Jobs & Pipelines**.

3. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```
   The `whoz_ingestion_refresh` job runs the pipeline every day
   (defined in resources/whoz_ingestion_refresh.job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

4. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

5. Finally, to run tests locally, use `pytest`:
   ```
   $ uv run pytest
   ```

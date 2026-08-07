"""Shared code for the Whoz source: everything more than one consumer needs.

Two consumers today, and the split between them is the reason this package exists:

  * the pipeline — src/whoz_ingestion_etl/transformations/**, which Lakeflow loads and
    which imports `whoz_ingestion.shaping.<entity>` and `whoz_ingestion.checks`
  * the test suite — tests/, which imports the very same modules

Nothing in this package may import `pyspark.pipelines`. That module only exists inside a
running Lakeflow pipeline, so importing it here would make every module in this package
uncollectable by pytest — which is precisely the property this package exists to keep.
Plain `pyspark.sql` only: DataFrame in, DataFrame out, plus the constants that describe
the result. (`checks.py` imports `databricks.labs.dqx`, which is fine — DQX is a plain
library, not a pipeline-only module, and validating checks needs no Spark session at all.)

  shaping/<entity>.py   `shape_<entity>(bronze) -> DataFrame`, plus that entity's
                        `<ENTITY>_COLUMNS` constants, rendered from schemas/
  shaping/<table>.py    one module per exploded child table, named for the entity that owns
                        it (profile_aptitudes, talent_workspace_history, …), each returning
                        SQL text that takes the source relation as an argument
  schemas/<entity>.yml  each table's columns, in order, as data
  contract.py           loads a schema file and renders it as a DDL string
  checks/<dataset>.yml  one dataset's data quality checks, as a native DQX check list
  checks.py             loads and validates them all, exposing CHECKS
  dq.py                 the pipeline's one lazily-built DQEngine

`dq.py` is the exception to "plain pyspark.sql only" below: it imports the Databricks SDK
and constructs a `WorkspaceClient`. That is safe here ONLY because the construction is behind
a function — importing this package must never need credentials, or pytest cannot collect.

The YAML files are read at import time, from disk, by path — so they must travel with the
package. They do: `databricks bundle deploy` syncs all of src/, and the editable install
resolves back to that same directory.

HOW IT RESOLVES AT RUNTIME. The pipeline's root_path is src/, not src/whoz_ingestion_etl/
(see resources/whoz_ingestion_etl.pipeline.yml), so src/ is on sys.path inside the pipeline
and this package is importable there by the same `whoz_ingestion.x` path pytest uses via
pyproject.toml's pythonpath. Keep those two settings in step: that they agree is what makes
a bad import fail locally instead of at deploy time.

This is also the package hatchling builds for `[project] name = "whoz_ingestion"`, which is
what `pip install --editable` installs when the pipeline sets up its environment (see the
same pipeline YAML's environment.dependencies).

Do not let this file become empty again. A zero-byte file is not deployed by
`databricks bundle deploy`, so the package silently vanishes from the workspace and the
editable install then fails the update with ENVIRONMENT_PIP_INSTALL_ERROR — with the
real cause (a missing directory) nowhere in the error text. Serverless caches pipeline
dependencies during development, so this can stay hidden for several runs before it
bites. Diagnosed the hard way; this docstring is what keeps the file non-empty.
"""

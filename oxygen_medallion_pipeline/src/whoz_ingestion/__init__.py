"""Shared code for the Whoz source: everything more than one consumer needs.

Two consumers today, and the split between them is the reason this package exists:

  * the pipeline — src/whoz_ingestion_etl/transformations/**, which Lakeflow loads and
    which imports `whoz_ingestion.shaping.<entity>` and `whoz_ingestion.expectations`
  * the test suite — tests/, which imports the very same modules

Nothing in this package may import `pyspark.pipelines`. That module only exists inside a
running Lakeflow pipeline, so importing it here would make every module in this package
uncollectable by pytest — which is precisely the property this package exists to keep.
Plain `pyspark.sql` only: DataFrame in, DataFrame out, plus the constants that describe
the result.

  shaping/<entity>.py   `shape_<entity>(bronze) -> DataFrame`, plus that entity's
                        `<ENTITY>_COLUMNS` DDL constants
  expectations.py       every data quality rule in the project, as {name: SQL} dicts

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

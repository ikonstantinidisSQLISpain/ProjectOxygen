"""Distribution package for the `whoz_ingestion` project defined in pyproject.toml.

Intentionally empty of logic. The pipeline's own code does NOT live here — it lives in
src/whoz_ingestion_etl/, which Lakeflow puts on sys.path directly via the pipeline's
root_path and which is therefore never imported as part of this package.

This module exists only so hatchling has a package matching `[project] name` to build
when the pipeline runs `pip install --editable` against the deployed bundle (see
resources/whoz_ingestion_etl.pipeline.yml's environment.dependencies).

Do not let this file become empty again. A zero-byte file is not deployed by
`databricks bundle deploy`, so the package silently vanishes from the workspace and the
editable install then fails the update with ENVIRONMENT_PIP_INSTALL_ERROR — with the
real cause (a missing directory) nowhere in the error text. Serverless caches pipeline
dependencies during development, so this can stay hidden for several runs before it
bites. Diagnosed the hard way; this docstring is what keeps the file non-empty.
"""

"""Per-entity row shaping: `whoz_ingestion.shaping.<entity>`.

One module per source entity, each exporting that entity's DDL constants and its
`shape_<entity>(bronze) -> DataFrame` function. Nothing here imports
`pyspark.pipelines`, which is what makes every module in this folder importable from a
plain pytest session — see the package docstring in ../__init__.py.

Deliberately not empty: a zero-byte file is not deployed by `databricks bundle deploy`,
which would make this a namespace-package gap in the workspace copy only. Same trap as
../__init__.py documents at length.
"""

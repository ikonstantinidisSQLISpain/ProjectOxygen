"""This file configures pytest and provides fixtures for a local Spark session and
loading test data.

Tests run against plain open-source PySpark, not a live Databricks workspace: the
logic under test (src/whoz_ingestion_etl/utilities/) only uses VARIANT / try_variant_get
/ variant_explode, which Apache Spark 4.0 open-sourced from Databricks Runtime. This
needs a local JDK (17+) on PATH but no Databricks credentials and no live cluster.
"""

import csv
import json
import os
import pathlib
import sys

import pytest
from pyspark.sql import SparkSession

# The JVM spawns a Python worker process and needs to be told which interpreter to
# use. Without this, on Windows in particular, it can't find a compatible one (PATH
# doesn't have the venv's python.exe under `uv run`) and every query times out with
# "Timed out while waiting for the Python worker to connect back". sys.executable is
# always the interpreter currently running pytest, so this is correct in any venv, on
# any OS, locally or in CI.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a local SparkSession fixture for tests.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    session = SparkSession.builder.appName("whoz_ingestion-tests").master("local[2]").getOrCreate()
    yield session
    session.stop()


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Example usage:

        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader

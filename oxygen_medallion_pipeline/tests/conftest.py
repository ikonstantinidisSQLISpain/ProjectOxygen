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
from pyspark.sql import DataFrame, SparkSession

# The JVM spawns a Python worker process and needs to be told which interpreter to
# use. Without this, on Windows in particular, it can't find a compatible one (PATH
# doesn't have the venv's python.exe under `uv run`) and every query times out with
# "Timed out while waiting for the Python worker to connect back". sys.executable is
# always the interpreter currently running pytest, so this is correct in any venv, on
# any OS, locally or in CI.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

FIXTURES_DIR = pathlib.Path(__file__).parent.parent / "fixtures"

# The miniature export that stands in for the real 101.6 MB file. See fixtures/README.md
# for what each of its 14 profiles is there to encode.
PROFILE_EXPORT_FIXTURE = "2020-01-01_whoz__profile_report_anonymized.json"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a local SparkSession fixture for tests.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    builder = SparkSession.builder.appName("oxygen_medallion_pipeline-tests")
    if not os.environ.get("SPARK_REMOTE"):
        builder = builder.master("local")
    session = builder.getOrCreate()
    yield session
    if not os.environ.get("SPARK_REMOTE"):
        session.stop()


@pytest.fixture(scope="session")
def fixture_profiles() -> list[dict]:
    """The profile export fixture as plain Python — for assertions about the file itself.

    Deliberately NOT a DataFrame: the tests that check the fixture still contains the
    hazards it claims to (tests/test_fixture_integrity.py) count with Python, independently
    of the Spark code path the fixture exists to feed. Two routes to the same number is the
    point; computing both with Spark would make the check circular.
    """
    return json.loads((FIXTURES_DIR / PROFILE_EXPORT_FIXTURE).read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def bronze_fixture(spark: SparkSession) -> DataFrame:
    """The profile export fixture, shaped exactly like bronze.whoz_profiles.

    Mirrors the *mechanism* in transformations/bronze_whoz_profiles.py, which is the part
    that has actually gone wrong before: the source is a pretty-printed JSON **array**, so
    the whole file lands as ONE VARIANT value in ONE row, and only variant_explode over
    that array turns it into one row per profile. Getting this wrong doesn't error — it
    silently yields a single row holding every profile, which is the bug that survived to
    the first live run (commit b95415e).

    Reading the file through pathlib rather than spark.read.text is deliberate: same
    one-row-holding-the-whole-file starting point, without dragging Hadoop path parsing
    (and the space in "OneDrive - SQLI") into every test run.

    Why not the plain json.loads + createDataFrame route: schema inference cannot read
    this data at all. `completionDetails` is an object on some records and an empty array
    on others, and createDataFrame gives up with CANNOT_INFER_TYPE_FOR_FIELD. That is
    hazard #1 in docs/whoz_profile_data_model.md, and it is why the pipeline is
    VARIANT-first — so the test loader has to be VARIANT-first too.

    NOTE: the projection below restates bronze_whoz_profiles.py's SELECT. When that SQL is
    extracted into utilities/ so it can be imported, this should call it instead — until
    then, a column added to bronze must be added here by hand.
    """
    text = (FIXTURES_DIR / PROFILE_EXPORT_FIXTURE).read_text(encoding="utf-8")

    # One row, one VARIANT, holding the entire array — the local equivalent of Auto
    # Loader's multiLine + singleVariantColumn.
    (
        spark.createDataFrame([(text,)], "raw string")
        .selectExpr("parse_json(raw) AS payload")
        .createOrReplaceTempView("_whoz_profiles_fixture_raw")
    )

    return spark.sql(f"""
        SELECT
            try_variant_get(e.value, '$.id', 'string')           AS profile_id,
            try_variant_get(e.value, '$.talentId', 'string')     AS talent_id,
            try_variant_get(e.value, '$.federationId', 'string') AS federation_id,
            e.value                                              AS payload,
            'dbfs:/Volumes/test/source/{PROFILE_EXPORT_FIXTURE}' AS source_file,
            '{PROFILE_EXPORT_FIXTURE}'                           AS source_file_name,
            CAST({len(text.encode("utf-8"))} AS BIGINT)          AS source_file_size,
            TIMESTAMP '2020-01-01 00:00:00'                      AS source_file_modified_at,
            current_timestamp()                                  AS ingested_at,
            current_date()                                       AS ingest_date,
            'whoz'                                               AS source_system,
            'profile_report'                                     AS source_entity,
            array_join(array_sort(map_keys(cast(e.value as map<string, variant>))), ',')
                                                                 AS payload_top_level_keys
        FROM _whoz_profiles_fixture_raw AS b,
             LATERAL variant_explode(b.payload) AS e
    """)


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Load a small, *non-polymorphic* JSON or CSV fixture by filename.

    Only safe for flat, uniformly-typed data you write yourself. It infers a schema from
    the Python objects, so it CANNOT read the profile export fixture — inference fails on
    completionDetails (object on some records, empty array on others). Use bronze_fixture
    for anything Whoz-shaped; this is for ad-hoc lookup tables and the like.

    Example usage:

        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str):
        path = FIXTURES_DIR / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text(encoding="utf-8"))
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader

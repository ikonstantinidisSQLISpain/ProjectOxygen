"""Shared pytest fixtures: a local Spark session, and bronze-shaped test input.

Tests run against plain open-source PySpark, not a live Databricks workspace: the logic
under test (src/whoz_ingestion/) only uses VARIANT / try_variant_get /
variant_explode / schema_of_variant, which Apache Spark 4.0 open-sourced from Databricks
Runtime. This needs a local JDK 17+ on PATH but no Databricks credentials and no live
cluster.

FIXTURES, per source entity. Adding an entity means adding one line to BRONZE_KEYS and two
one-line fixtures at the bottom of this file — nothing else here changes:

    profile_fixture("hazards")   -> bronze.whoz_profiles-shaped DataFrame from a file
    talent_fixture("hazards")    -> bronze.whoz_talents-shaped DataFrame from a file
    profile_bronze(records)      -> the same, from inline dicts
    talent_bronze(records)

So a test reads either

    def test_something(talent_fixture):
        result = shape_talent(talent_fixture("hazards"))

for a stored fixture file, or

    def test_one_specific_thing(talent_bronze):
        result = shape_talent(talent_bronze([{"id": "t1", "profile": {"id": "p1"}}]))

for a case small and pointed enough that inlining is clearer than a file. Prefer the file
when the records describe the *source* ("this is what Whoz sends"), and inline when they
describe the *test* ("this row has one field set, to isolate one behaviour").
"""

import json
import os
import pathlib
import sys
from collections.abc import Callable
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# The JVM spawns a Python worker process and needs to be told which interpreter to
# use. Without this, on Windows in particular, it can't find a compatible one (PATH
# doesn't have the venv's python.exe under `uv run`) and every query times out with
# "Timed out while waiting for the Python worker to connect back". sys.executable is
# always the interpreter currently running pytest, so this is correct in any venv, on
# any OS, locally or in CI.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

FIXTURE_ROOT = pathlib.Path(__file__).parent.parent / "fixtures"

# Fixed, not current_timestamp()/current_date(): every value a test sees should be
# reproducible, so a failure means a real change and never the clock. Nothing asserts on
# these two, they exist because bronze carries them and the shaping functions pass them
# through — but the day something does assert on them, it can.
INGESTED_AT = "2026-07-30 12:00:00"

# Per entity, because the lineage column should look like the file that entity really lands
# under — these mirror the FILE_NAME_GLOB in each transformations/bronze_*.py.
SOURCE_FILES = {
    "whoz_profiles": "dbfs:/Volumes/test/landing/source/2026-07-30_whoz__profile_report_anonymized.json",
    "whoz_talents": "dbfs:/Volumes/test/landing/source/2026-07-30_whoz__talent_report_anonymized.json",
}

# The identity columns each bronze table pulls out of its raw payload, as
# {column name: JSON path}. This is the ONLY entity-specific thing in this file, and it
# mirrors the try_variant_get calls at the top of each transformations/bronze_*.py — keep
# the two in step, since a test building input by a different path than production reads it
# is a test that can pass while production is broken.
BRONZE_KEYS: dict[str, dict[str, str]] = {
    "whoz_profiles": {
        "profile_id": "$.id",
        "talent_id": "$.talentId",
        "federation_id": "$.federationId",
    },
    "whoz_talents": {
        "talent_id": "$.id",
        "federation_id": "$.federationId",
        "user_id": "$.userId",
        "workspace_id": "$.workspaceId",
    },
}


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A local SparkSession, started once for the whole test session.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    builder = SparkSession.builder.appName("oxygen_medallion_pipeline-tests")
    if not os.environ.get("SPARK_REMOTE"):
        builder = builder.master("local[2]")
    session = builder.getOrCreate()
    # Pinned, and it matters: the source's "2025-01-21T15:05:20.930Z" otherwise renders as
    # 16:05 on a laptop in Europe/Madrid and 15:05 on GitHub's UTC runners, so a test
    # asserting a timestamp passes locally and fails in CI. UTC is also what the source
    # sends, so fixtures and assertions can be read off each other directly.
    #
    # This governs Spark-side rendering only. df.collect() converts TIMESTAMPs to Python
    # datetimes using the JVM default zone instead, which this setting does not reach —
    # measured, not assumed. Tests that assert on a timestamp must go through
    # helpers.to_utc_strings(); see its docstring for the numbers.
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    if not os.environ.get("SPARK_REMOTE"):
        session.stop()


@pytest.fixture()
def source_records() -> Callable[[str, str], list[dict[str, Any]]]:
    """Load fixtures/<entity>/<name>.json as a plain list of dicts.

    The files hold a JSON *array* of source objects — the same shape as the real Whoz
    export, so a fixture can be a trimmed copy of real records without reformatting.
    """

    def _load(entity: str, name: str) -> list[dict[str, Any]]:
        path = (FIXTURE_ROOT / entity / name).with_suffix(".json")
        if not path.exists():
            available = sorted(p.stem for p in (FIXTURE_ROOT / entity).glob("*.json"))
            raise FileNotFoundError(f"no such {entity} fixture: {name!r}. Available: {available}")
        records = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(records, list):
            raise TypeError(f"{path.name} must hold a JSON array of source objects, got {type(records).__name__}")
        return records

    return _load


@pytest.fixture()
def bronze_of(spark: SparkSession) -> Callable[[list[dict[str, Any]], str], DataFrame]:
    """Turn source dicts into a DataFrame shaped like that entity's bronze table.

    This is the single place in the suite that knows how to build a real VARIANT column,
    and it exists because getting it wrong is easy and the failure is baffling. Three rules,
    all learned the hard way:

      1. Build the whole thing in one .select(...) chain off a JVM-side relation. Do NOT
         round-trip a VARIANT value through a local Row (via .first(), or by collecting
         and re-creating): Spark loses the logical VARIANT type and re-infers the physical
         STRUCT<metadata: BINARY, value: BINARY> layout, and try_variant_get then fails
         with DATATYPE_MISMATCH even though the JSON is perfectly fine.
      2. Derive the identity columns from the payload with try_variant_get, exactly as the
         bronze transformation does, rather than letting the test pass them in alongside. A
         fixture record then cannot disagree with itself, and a record with a null id
         produces a null key here for the same reason it would in production.
      3. Do NOT use spark.createDataFrame(rows, ...) here — the reason for the odd-looking
         range(1) + explode(array(lit, lit, ...)) below. createDataFrame on local Python
         data builds a *Python* RDD, so every Spark job over it makes the JVM launch a
         python.exe worker to deserialize the pickled rows. On Linux those workers are
         forked from a reusable daemon and cost nothing; pyspark.daemon is POSIX-only, so
         on Windows each task spawns a fresh interpreter out of the venv instead. Measured
         on this repo, 10 records, one collect():

             createDataFrame (Python RDD)   2.57s per job
             range + explode (JVM only)     0.09s per job

         The suite runs ~70 jobs, which is the difference between a 166s run and a ~20s
         one. Nothing else about the DataFrame changes — same columns, same types, same
         VARIANT. Fixtures are a few KB, so inlining them as literals in the query plan is
         free; if one ever grows to megabytes, switch to spark.read.text() of a temp file,
         which is also JVM-side, rather than going back to createDataFrame.

    Columns are bronze's: the entity's keys (BRONZE_KEYS), the raw payload, and the two
    lineage columns. Add more from the bronze transformation's SELECT when a test needs them.
    """

    def _build(records: list[dict[str, Any]], entity: str) -> DataFrame:
        payloads = [json.dumps(record) for record in records]
        if payloads:
            # One row per record, materialized entirely inside the JVM. range(1) is just a
            # single-row seed for the explode; it contributes no columns of its own.
            raw = spark.range(1).select(
                F.explode(F.array(*[F.lit(payload) for payload in payloads])).alias("payload_json")
            )
        else:
            # The empty case can't go through explode(array()) — with no elements Spark
            # infers array<void>, and parse_json on a VOID column fails to analyse. An
            # empty range with an explicitly typed column gives zero rows AND the right
            # schema, so shape_*() analyses exactly as it does for a populated fixture.
            raw = spark.range(0).select(F.lit(None).cast("string").alias("payload_json"))

        keys = [
            F.try_variant_get("payload", path, "string").alias(column) for column, path in BRONZE_KEYS[entity].items()
        ]
        return raw.select(F.parse_json("payload_json").alias("payload")).select(
            *keys,
            F.col("payload"),
            F.lit(SOURCE_FILES[entity]).alias("source_file"),
            F.lit(INGESTED_AT).cast("timestamp").alias("ingested_at"),
        )

    return _build


# -------------------------------------------------------------------------------------
# Per-entity conveniences. Two lines each; this is the whole cost of adding an entity.
# -------------------------------------------------------------------------------------
@pytest.fixture()
def profile_bronze(bronze_of) -> Callable[[list[dict[str, Any]]], DataFrame]:
    """Profile dicts -> a bronze.whoz_profiles-shaped DataFrame."""
    return lambda records: bronze_of(records, "whoz_profiles")


@pytest.fixture()
def profile_fixture(source_records, profile_bronze) -> Callable[[str], DataFrame]:
    """fixtures/whoz_profiles/<name>.json -> a bronze.whoz_profiles-shaped DataFrame."""
    return lambda name: profile_bronze(source_records("whoz_profiles", name))


@pytest.fixture()
def talent_bronze(bronze_of) -> Callable[[list[dict[str, Any]]], DataFrame]:
    """Talent dicts -> a bronze.whoz_talents-shaped DataFrame."""
    return lambda records: bronze_of(records, "whoz_talents")


@pytest.fixture()
def talent_fixture(source_records, talent_bronze) -> Callable[[str], DataFrame]:
    """fixtures/whoz_talents/<name>.json -> a bronze.whoz_talents-shaped DataFrame."""
    return lambda name: talent_bronze(source_records("whoz_talents", name))

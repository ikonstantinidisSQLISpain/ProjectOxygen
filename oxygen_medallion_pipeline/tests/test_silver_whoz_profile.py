"""Unit tests for the pure transform in utilities/profile_shaping.py.

@dp.table-decorated functions can't be imported outside of a running pipeline —
`from pyspark import pipelines` doesn't exist in a plain databricks-connect install,
it's only injected inside an actual pipeline execution. So the shaping logic lives in
utilities/profile_shaping.py, which has no pipelines dependency at all, and
silver_whoz_profile.py just imports and wraps it with @dp.table.

These tests build a tiny bronze-shaped DataFrame by hand, using parse_json to get real
VARIANT payload columns, and assert on the two type hazards documented in
docs/whoz_profile_data_model.md: completionRate arriving as either an int or a float,
and headline.aim being present-but-null vs the whole headline object being absent.

IMPORTANT: build the whole DataFrame in one createDataFrame(...).select(...) call, not
row by row via .first()/Row reassembly. Round-tripping a VARIANT value through a local
Row object loses its type: Spark falls back to inferring the physical
STRUCT<metadata: BINARY, value: BINARY> layout instead of the logical VARIANT type, and
try_variant_get then fails with DATATYPE_MISMATCH. Confirmed by hand while writing this.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from whoz_ingestion_etl.utilities.profile_shaping import shape_profile


def _bronze_df(spark: SparkSession, rows: list[tuple[str, str, str]]) -> DataFrame:
    """rows: (profile_id, talent_id, payload_json) tuples -> bronze-shaped DataFrame."""
    return (
        spark.createDataFrame(rows, "profile_id string, talent_id string, payload_json string")
        .select(
            "profile_id",
            "talent_id",
            F.lit("fed-1").alias("federation_id"),
            F.parse_json("payload_json").alias("payload"),
            F.lit("dbfs:/whoz/profiles/test.json").alias("source_file"),
            F.current_timestamp().alias("ingested_at"),
        )
    )


def test_completion_rate_normalizes_int_and_float_to_double(spark: SparkSession):
    bronze = _bronze_df(spark, [
        ("p1", "t1", '{"id": "p1", "talentId": "t1", "main": true, "completionRate": 42}'),
        ("p2", "t2", '{"id": "p2", "talentId": "t2", "main": true, "completionRate": 42.5}'),
    ])

    result = shape_profile(bronze).select("profile_id", "completion_rate").orderBy("profile_id").collect()

    assert result[0]["completion_rate"] == 42.0
    assert result[1]["completion_rate"] == 42.5
    assert dict(shape_profile(bronze).dtypes)["completion_rate"] == "double"


def test_absent_headline_vs_null_field_both_resolve_to_null(spark: SparkSession):
    # headline.aim is present but null on every real record; headline itself is
    # entirely absent on ~25% of profiles. Both should surface as NULL in silver —
    # the VARIANT-preserved absent-vs-null distinction only matters on the raw
    # payload column, not on the flattened columns.
    bronze = _bronze_df(spark, [
        ("p3", "t3", '{"id": "p3", "talentId": "t3", "main": true}'),
        ("p4", "t4", '{"id": "p4", "talentId": "t4", "main": true, '
                      '"headline": {"aim": null, "jobTitle": "Engineer"}}'),
    ])

    result = (
        shape_profile(bronze)
        .select("profile_id", "headline_aim", "headline_job_title")
        .orderBy("profile_id")
        .collect()
    )

    assert result[0]["headline_aim"] is None
    assert result[0]["headline_job_title"] is None
    assert result[1]["headline_aim"] is None
    assert result[1]["headline_job_title"] == "Engineer"


def test_missing_path_nulls_the_column_instead_of_failing(spark: SparkSession):
    # try_variant_get on a genuinely malformed/sparse record must not raise.
    bronze = _bronze_df(spark, [("p5", "t5", '{"id": "p5", "talentId": "t5", "main": true}')])

    row = shape_profile(bronze).first()

    assert row["profile_id"] == "p5"
    assert row["status"] is None
    assert row["completion_rate"] is None

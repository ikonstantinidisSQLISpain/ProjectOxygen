# =====================================================================================
# SILVER — silver.whoz_profile_aptitudes (+ its quarantine table)
#
# The skills inventory: one row per aptitude declared on a profile, exploded out of the
# bronze VARIANT payload's aptitudes[].
#
# Wiring only. This file decides which bronze relation to read and which check list to
# enforce; the query is in whoz_ingestion/shaping/profile_aptitudes.py, where a test can
# reach it, and the checks are data in whoz_ingestion/checks/whoz_profile_aptitudes.yml.
# The three-object shape below is documented once in the root README, under
# "How a checked dataset is wired".
# =====================================================================================

from pyspark import pipelines as dp

from whoz_ingestion.checks import CHECKS
from whoz_ingestion.dq import engine
from whoz_ingestion.shaping.profile_aptitudes import aptitudes_sql

# See bronze/whoz_profiles.py — resources/whoz_ingestion_etl.pipeline.yml is the only source
# of truth for these, so no fallback value here: a deploy missing this config should fail
# loudly rather than silently land on someone else's catalog.
CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles"

# One engine for the whole pipeline, not one per module. Constructing a DQEngine verifies
# workspace connectivity as a side effect — twice per construction — so a shared instance is
# what keeps this silver layer at 2 control-plane round trips rather than one pair per file.
# See whoz_ingestion/dq.py.
dq = engine(spark)


@dp.temporary_view
def whoz_profile_aptitudes_checked():
    return dq.apply_checks_by_metadata(
        spark.sql(aptitudes_sql(f"STREAM({BRONZE_TABLE})")), CHECKS["whoz_profile_aptitudes"]
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_aptitudes",
    comment="One row per aptitude (skill / language / tool) declared on a profile.",
    table_properties={"quality": "silver"},
    cluster_by=["profile_id", "aptitude_type"],
)
def whoz_profile_aptitudes():
    return dq.get_valid(spark.readStream.table("whoz_profile_aptitudes_checked"))


# No explicit schema= on any quarantine table in this pipeline: it is the base columns plus
# DQX's two result arrays, whose nested struct DQX owns and may extend between minor releases,
# and no AUTO CDC flow reads these tables so nothing depends on their column order. See the
# longer note on whoz_profiles_quarantine in silver/whoz_profile.py.
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_aptitudes_quarantine",
    comment=(
        "Aptitude rows that failed a DQX check. Rows with _errors were kept OUT of "
        "silver.whoz_profile_aptitudes; rows with only _warnings are in both."
    ),
    table_properties={"quality": "quarantine"},
)
def whoz_profile_aptitudes_quarantine():
    return dq.get_invalid(spark.readStream.table("whoz_profile_aptitudes_checked"))

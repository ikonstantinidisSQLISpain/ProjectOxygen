# =====================================================================================
# SILVER — silver.whoz_profile_positions (+ its quarantine table)
#
# Jobs and missions in one array, split by is_mission and linked by parent_position_id.
# Self-hierarchical: all 1,197 parent references resolve within the file.
#
# Wiring only. This file decides which bronze relation to read and which check list to
# enforce; the query is in whoz_ingestion/shaping/profile_positions.py, where a test can
# reach it, and the checks are data in whoz_ingestion/checks/whoz_profile_positions.yml.
# The three-object shape below is documented once in the root README, under
# "How a checked dataset is wired".
# =====================================================================================

from pyspark import pipelines as dp

from whoz_ingestion.checks import CHECKS
from whoz_ingestion.dq import engine
from whoz_ingestion.shaping.profile_positions import positions_sql

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles"

# Shared across every module in this pipeline — see whoz_ingestion/dq.py.
dq = engine(spark)


@dp.temporary_view
def whoz_profile_positions_checked():
    return dq.apply_checks_by_metadata(
        spark.sql(positions_sql(f"STREAM({BRONZE_TABLE})")), CHECKS["whoz_profile_positions"]
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_positions",
    comment="One row per position (job or mission) held on a profile. Self-hierarchical.",
    table_properties={"quality": "silver"},
    cluster_by=["profile_id", "start_date"],
)
def whoz_profile_positions():
    return dq.get_valid(spark.readStream.table("whoz_profile_positions_checked"))


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_positions_quarantine",
    comment=(
        "Position rows that failed a DQX check. Rows with _errors were kept OUT of "
        "silver.whoz_profile_positions; rows with only _warnings are in both."
    ),
    table_properties={"quality": "quarantine"},
)
def whoz_profile_positions_quarantine():
    return dq.get_invalid(spark.readStream.table("whoz_profile_positions_checked"))

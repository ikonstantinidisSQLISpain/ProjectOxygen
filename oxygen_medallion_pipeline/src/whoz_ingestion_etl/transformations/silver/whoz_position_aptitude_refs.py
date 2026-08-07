# =====================================================================================
# SILVER — silver.whoz_position_aptitude_refs (+ its quarantine table)
#
# The position -> aptitude bridge. No natural id on the child, so the grain is the pair.
# 137,050 references against 113,810 aptitudes: the same skill is claimed on several
# positions, which is the point of the table.
#
# Wiring only. This file decides which bronze relation to read and which check list to
# enforce; the query is in whoz_ingestion/shaping/profile_position_aptitude_refs.py, where a
# test can reach it, and the checks are data in
# whoz_ingestion/checks/whoz_position_aptitude_refs.yml. The three-object shape below is
# documented once in the root README, under "How a checked dataset is wired".
# =====================================================================================

from pyspark import pipelines as dp

from whoz_ingestion.checks import CHECKS
from whoz_ingestion.dq import engine
from whoz_ingestion.shaping.profile_position_aptitude_refs import aptitude_refs_sql

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles"

# Shared across every module in this pipeline — see whoz_ingestion/dq.py.
dq = engine(spark)


@dp.temporary_view
def whoz_position_aptitude_refs_checked():
    return dq.apply_checks_by_metadata(
        spark.sql(aptitude_refs_sql(f"STREAM({BRONZE_TABLE})")), CHECKS["whoz_position_aptitude_refs"]
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_position_aptitude_refs",
    comment="Bridge: skills claimed on each position. Grain = (position_id, aptitude_id).",
    table_properties={"quality": "silver"},
    cluster_by=["profile_id", "position_id"],
)
def whoz_position_aptitude_refs():
    return dq.get_valid(spark.readStream.table("whoz_position_aptitude_refs_checked"))


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_position_aptitude_refs_quarantine",
    comment=(
        "Bridge rows that failed a DQX check — in practice a null half of the "
        "(position_id, aptitude_id) key, which is `error` and so withheld from the table."
    ),
    table_properties={"quality": "quarantine"},
)
def whoz_position_aptitude_refs_quarantine():
    return dq.get_invalid(spark.readStream.table("whoz_position_aptitude_refs_checked"))

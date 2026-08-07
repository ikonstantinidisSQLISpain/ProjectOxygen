# =====================================================================================
# SILVER — silver.whoz_talent_workspace_history (+ its quarantine table)
#
# The talent export's own history[] array: which workspace the talent belonged to, since
# when, and under which scope. One row per (talent, membership period).
#
# NOT silver.whoz_talent_versions, which is the SCD2 history of the talent *record* and is
# derived by AUTO CDC in silver/whoz_talent.py. Two different things; the names are
# deliberately not both "history".
#
# Wiring only. This file decides which bronze relation to read and which check list to
# enforce; the query is in whoz_ingestion/shaping/talent_workspace_history.py, where a test
# can reach it, and the checks are data in
# whoz_ingestion/checks/whoz_talent_workspace_history.yml. The three-object shape below is
# documented once in the root README, under "How a checked dataset is wired".
# =====================================================================================

from pyspark import pipelines as dp

from whoz_ingestion.checks import CHECKS
from whoz_ingestion.dq import engine
from whoz_ingestion.shaping.talent_workspace_history import workspace_history_sql

# Note the bronze table: this is the ONLY child dataset in the pipeline that explodes out of
# bronze.whoz_talents rather than bronze.whoz_profiles.
CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_talents"

# Shared across every module in this pipeline — see whoz_ingestion/dq.py.
dq = engine(spark)


@dp.temporary_view
def whoz_talent_workspace_history_checked():
    return dq.apply_checks_by_metadata(
        spark.sql(workspace_history_sql(f"STREAM({BRONZE_TABLE})")),
        CHECKS["whoz_talent_workspace_history"],
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_talent_workspace_history",
    comment="One row per workspace membership period of a talent, from the source's history[] array.",
    table_properties={"quality": "silver"},
    cluster_by=["talent_id"],
)
def whoz_talent_workspace_history():
    return dq.get_valid(spark.readStream.table("whoz_talent_workspace_history_checked"))


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_talent_workspace_history_quarantine",
    comment=(
        "Workspace-history rows that failed a DQX check. Rows with _errors were kept OUT of "
        "silver.whoz_talent_workspace_history; rows with only _warnings are in both."
    ),
    table_properties={"quality": "quarantine"},
)
def whoz_talent_workspace_history_quarantine():
    return dq.get_invalid(spark.readStream.table("whoz_talent_workspace_history_checked"))

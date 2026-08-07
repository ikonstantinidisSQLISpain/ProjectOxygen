# =====================================================================================
# SILVER — silver.whoz_user_workspace_roles (+ its quarantine table)
#
# Which workspaces a user account belongs to, and as what. Grain is
# (user_id, workspace_id, role) — 15,258 memberships over 2,858 of 4,036 users.
#
# Exploded out of the `workspaceRoles` map, which arrives as an empty ARRAY on the 1,178
# accounts with no memberships. The query handles both forms; see
# whoz_ingestion/shaping/user_workspace_roles.py for why there is no type test.
#
# Wiring only. This file decides which bronze relation to read and which check list to
# enforce; the query is in the shaping module above, where a test can reach it, and the checks
# are data in whoz_ingestion/checks/whoz_user_workspace_roles.yml. The three-object shape
# below is documented once in the root README, under "How a checked dataset is wired".
# =====================================================================================

from pyspark import pipelines as dp

from whoz_ingestion.checks import CHECKS
from whoz_ingestion.dq import engine
from whoz_ingestion.shaping.user_workspace_roles import workspace_roles_sql

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_users"

# Shared across every module in this pipeline — see whoz_ingestion/dq.py.
dq = engine(spark)


@dp.temporary_view
def whoz_user_workspace_roles_checked():
    return dq.apply_checks_by_metadata(
        spark.sql(workspace_roles_sql(f"STREAM({BRONZE_TABLE})")),
        CHECKS["whoz_user_workspace_roles"],
    )


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_user_workspace_roles",
    comment=(
        "One row per (user, workspace, role). Exploded to one row per role rather than "
        "lifting roles[0], so a second role on a membership arrives as a row and not as "
        "silently dropped data."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["user_id", "workspace_id"],
)
def whoz_user_workspace_roles():
    return dq.get_valid(spark.readStream.table("whoz_user_workspace_roles_checked"))


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_user_workspace_roles_quarantine",
    comment=(
        "Workspace-role rows that failed a DQX check. Rows with _errors were kept OUT of "
        "silver.whoz_user_workspace_roles; rows with only _warnings are in both."
    ),
    table_properties={"quality": "quarantine"},
)
def whoz_user_workspace_roles_quarantine():
    return dq.get_invalid(spark.readStream.table("whoz_user_workspace_roles_checked"))

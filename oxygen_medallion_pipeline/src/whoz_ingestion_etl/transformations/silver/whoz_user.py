# =====================================================================================
# SILVER — silver.whoz_users / silver.whoz_user_versions
#
# Account-level (1 row per user account) flattening of the bronze VARIANT payload. This file
# holds ONLY the user grain; workspace membership is its own file,
# silver/whoz_user_workspace_roles.py.
#
# A user is the LOGIN ACCOUNT, not the person: silver.whoz_talents is a person in a workspace,
# and the two are not 1:1 in either direction (1,760 users have no talent; 1,841 talents have
# no user). The foreign key lives on the talent side and stays there — this table carries no
# talent_id. See whoz_ingestion/shaping/user.py's header and docs/whoz_user_data_model.md §1.
#
# NAMING: whoz_user_versions is the SCD2 history of this table, matching whoz_talent_versions.
# Unlike the talent entity there is no second "history" concept on users, so the ambiguity
# that forced whoz_talent_workspace_history's careful naming does not arise here.
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from whoz_ingestion.checks import CHECKS
from whoz_ingestion.dq import engine
from whoz_ingestion.shaping.user import USER_COLUMNS, USER_HISTORY_COLUMNS, shape_user

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_users"

# Shared across every module in this pipeline, and lazily built — see whoz_ingestion/dq.py
# for why constructing a DQEngine is a live workspace call and why only one is wanted.
dq = engine(spark)


# -------------------------------------------------------------------------------------
# Checked rows off bronze — shaped, then annotated by DQX with _errors and _warnings.
# Defined once here; the valid view and the quarantine table below are both defined against
# it. Defined once, not *evaluated* once — a pipeline view is recomputed per consumer. See
# the note on whoz_profile_checked in silver/whoz_profile.py for why that matters.
#
# The check list is data in whoz_ingestion/checks/whoz_user_shaped.yml, keyed by that file's
# name, so tests/layer3_rules/test_user_rules.py can apply the identical list to real
# shape_user() output without a running pipeline.
# -------------------------------------------------------------------------------------
@dp.temporary_view
def whoz_user_checked():
    return dq.apply_checks_by_metadata(shape_user(spark.readStream.table(BRONZE_TABLE)), CHECKS["whoz_user_shaped"])


# -------------------------------------------------------------------------------------
# The rows that pass. Both AUTO CDC flows below read this same view, so one check list
# protects both targets. get_valid() drops the two result columns, so the schema those flows
# merge into is exactly USER_COLUMNS.
# -------------------------------------------------------------------------------------
@dp.temporary_view
def whoz_user_shaped():
    return dq.get_valid(spark.readStream.table("whoz_user_checked"))


# -------------------------------------------------------------------------------------
# The rows that did not. `warn` rows are in here AND in silver.whoz_users; filter on
# `_errors IS NOT NULL` for the ones actually withheld. No explicit schema= — see the note on
# whoz_profiles_quarantine in silver/whoz_profile.py.
#
# EXPECT EXACTLY ONE `_errors` ROW HERE on a full refresh of the analysed export: it carries a
# single all-null record, which user_id_not_null withholds. That is correct, not a bug.
# -------------------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_users_quarantine",
    comment=(
        "User rows that failed a DQX check, with _errors/_warnings naming which. Rows with "
        "_errors were kept OUT of silver.whoz_users; rows with only _warnings are in both."
    ),
    table_properties={"quality": "quarantine"},
)
def whoz_users_quarantine():
    return dq.get_invalid(spark.readStream.table("whoz_user_checked"))


# whoz_users — SCD Type 1: one row per user_id, current state only.
dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_users",
    comment=(
        "One row per Whoz user ACCOUNT, current state only — see whoz_user_versions for prior "
        "versions. Not a person: join from silver.whoz_talents.user_id, and note that 44% of "
        "accounts have no talent and 45% of talents have no account."
    ),
    schema=USER_COLUMNS,
    table_properties={"quality": "silver"},
    cluster_by=["federation_id", "user_id"],
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.whoz_users",
    source="whoz_user_shaped",
    keys=["user_id"],
    # Whoz's own last-modified time on the record, not when we happened to ingest it —
    # protects against an older dated export landing after a newer one (e.g. backfill).
    sequence_by=F.col("source_last_modified_at"),
    stored_as_scd_type="1",
    # No except_column_list needed: shape_user does not emit the payload column, and VARIANT
    # is what breaks AUTO CDC's whole-row `<=>` comparison (INVALID_ORDERING_TYPE).
)

# whoz_user_versions — SCD Type 2: every version of every account record.
dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_user_versions",
    comment=(
        "Full version history of whoz_users, one row per (user_id, version). "
        "__START_AT/__END_AT mark each version's validity window; NULL __END_AT is current."
    ),
    schema=USER_HISTORY_COLUMNS,
    table_properties={"quality": "silver"},
    cluster_by=["user_id"],
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.whoz_user_versions",
    source="whoz_user_shaped",
    keys=["user_id"],
    sequence_by=F.col("source_last_modified_at"),
    stored_as_scd_type="2",
)

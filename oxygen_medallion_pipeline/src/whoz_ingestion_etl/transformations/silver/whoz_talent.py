# =====================================================================================
# SILVER — silver.whoz_talents / silver.whoz_talent_versions
#
# Talent-level (1 row per talent) flattening of the bronze VARIANT payload. This file holds
# ONLY the talent grain; the workspace membership history exploded out of history[] is its
# own file, silver/whoz_talent_workspace_history.py.
#
# The nested `profile` object is NOT re-modelled here: silver.whoz_profiles already models
# profiles from the separate profile export, and this table carries profile_id as a foreign
# key into it. See whoz_ingestion/shaping/talent.py's header for that decision.
#
# NAMING: whoz_talent_versions is the SCD2 history of *this table* (how a talent record
# changed over time), and it is defined below. whoz_talent_workspace_history is the source's
# own history[] array (which workspaces the talent has belonged to), and it is the separate
# file named above. Two different things; the names are deliberately not both "history", and
# splitting them into two files makes that harder to miss than a heading did.
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# The shared code lives in the sibling src/whoz_ingestion/ package: the pipeline's
# root_path IS src/, so src/ itself is on sys.path at runtime and `whoz_ingestion.x`
# resolves — the same path the test suite imports by.
from whoz_ingestion.checks import CHECKS
from whoz_ingestion.dq import engine
from whoz_ingestion.shaping.talent import TALENT_COLUMNS, TALENT_HISTORY_COLUMNS, shape_talent

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_talents"

# Shared across every module in this pipeline, and lazily built — see whoz_ingestion/dq.py
# for why constructing a DQEngine is a live workspace call and why only one is wanted.
dq = engine(spark)


# -------------------------------------------------------------------------------------
# Checked rows off bronze — shaped, then annotated by DQX with _errors and _warnings.
# Defined once here; the valid view and the quarantine table below are both defined against
# it. Defined once, not *evaluated* once — a pipeline view is recomputed per consumer, so
# this expression runs once per downstream flow, each with its own checkpoint. See the note
# on whoz_profile_checked in silver/whoz_profile.py for why that matters.
#
# The check list is data in whoz_ingestion/checks/whoz_talent_shaped.yml, keyed by that
# file's name, so tests/layer3_rules/test_talent_rules.py can apply the identical list to
# real shape_talent() output without a running pipeline.
# -------------------------------------------------------------------------------------
@dp.temporary_view
def whoz_talent_checked():
    return dq.apply_checks_by_metadata(shape_talent(spark.readStream.table(BRONZE_TABLE)), CHECKS["whoz_talent_shaped"])


# -------------------------------------------------------------------------------------
# The rows that pass. Both AUTO CDC flows below read this same view, so one check list
# protects both targets — written once, though evaluated once per flow. The name is
# unchanged from before DQX and get_valid() drops the two result columns, so the schema
# those flows merge into is exactly what it was.
# -------------------------------------------------------------------------------------
@dp.temporary_view
def whoz_talent_shaped():
    return dq.get_valid(spark.readStream.table("whoz_talent_checked"))


# -------------------------------------------------------------------------------------
# The rows that did not. `warn` rows are in here AND in silver.whoz_talents; filter on
# `_errors IS NOT NULL` for the ones actually withheld. No explicit schema= — see the note
# on whoz_profiles_quarantine in silver/whoz_profile.py.
# -------------------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_talents_quarantine",
    comment=(
        "Talent rows that failed a DQX check, with _errors/_warnings naming which. Rows with "
        "_errors were kept OUT of silver.whoz_talents; rows with only _warnings are in both."
    ),
    table_properties={"quality": "quarantine"},
)
def whoz_talents_quarantine():
    return dq.get_invalid(spark.readStream.table("whoz_talent_checked"))


# whoz_talents — SCD Type 1: one row per talent_id, current state only.
dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_talents",
    comment=(
        "One row per Whoz talent, current state only — see whoz_talent_versions for prior "
        "versions. profile_id joins to silver.whoz_profiles; query bronze.whoz_talents by "
        "talent_id for the raw nested profile JSON."
    ),
    schema=TALENT_COLUMNS,
    table_properties={"quality": "silver"},
    cluster_by=["federation_id", "talent_id"],
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.whoz_talents",
    source="whoz_talent_shaped",
    keys=["talent_id"],
    # Whoz's own last-modified time on the record, not when we happened to ingest it —
    # protects against an older dated export landing after a newer one (e.g. backfill).
    sequence_by=F.col("source_last_modified_at"),
    stored_as_scd_type="1",
    # No except_column_list needed: shape_talent does not emit the payload column, and
    # VARIANT is what breaks AUTO CDC's whole-row `<=>` comparison (INVALID_ORDERING_TYPE).
)

# whoz_talent_versions — SCD Type 2: every version of every talent record.
dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_talent_versions",
    comment=(
        "Full version history of whoz_talents, one row per (talent_id, version). "
        "__START_AT/__END_AT mark each version's validity window; NULL __END_AT is current."
    ),
    schema=TALENT_HISTORY_COLUMNS,
    table_properties={"quality": "silver"},
    cluster_by=["talent_id"],
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.whoz_talent_versions",
    source="whoz_talent_shaped",
    keys=["talent_id"],
    sequence_by=F.col("source_last_modified_at"),
    stored_as_scd_type="2",
)

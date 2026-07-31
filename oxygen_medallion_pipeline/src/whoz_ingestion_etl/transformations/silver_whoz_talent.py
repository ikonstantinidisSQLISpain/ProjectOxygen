# =====================================================================================
# SILVER — silver.whoz_talents / silver.whoz_talent_versions
#
# Talent-level (1 row per talent) flattening of the bronze VARIANT payload, plus the
# workspace membership history exploded out of history[].
#
# The nested `profile` object is NOT re-modelled here: silver.whoz_profiles already models
# profiles from the separate profile export, and this table carries profile_id as a foreign
# key into it. See utilities/talent_shaping.py's header for that decision.
#
# NAMING: whoz_talent_versions is the SCD2 history of *this table* (how a talent record
# changed over time). whoz_talent_workspace_history is the source's own history[] array
# (which workspaces the talent has belonged to). Two different things; the names are
# deliberately not both "history".
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Not "whoz_ingestion_etl.utilities...": the pipeline's root_path IS src/whoz_ingestion_etl,
# so that folder itself is on sys.path at runtime, not its parent.
from utilities.expectations import (
    TALENT_MUST_HOLD,
    TALENT_SHOULD_HOLD,
    WORKSPACE_HISTORY_MUST_HOLD,
    WORKSPACE_HISTORY_SHOULD_HOLD,
)
from utilities.talent_shaping import TALENT_COLUMNS, TALENT_HISTORY_COLUMNS, shape_talent

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_talents"


# -------------------------------------------------------------------------------------
# Shaped, validated rows off bronze — pipeline-scoped, materializes nothing itself. Both
# AUTO CDC flows below read this same view, so the rules run once and protect both targets.
#
# Rule sets live in utilities/expectations.py so tests/test_expectations.py can evaluate
# every predicate against real shape_talent() output.
# -------------------------------------------------------------------------------------
@dp.temporary_view
@dp.expect_all_or_drop(TALENT_MUST_HOLD)
@dp.expect_all(TALENT_SHOULD_HOLD)
def whoz_talent_shaped():
    return shape_talent(spark.readStream.table(BRONZE_TABLE))


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


# =====================================================================================
# SILVER — silver.whoz_talent_workspace_history
#
# The source's history[] array: which workspace the talent belonged to, since when, and
# under which scope. One row per (talent, membership period).
#
# variant_explode is a table-valued generator, so it goes in the FROM clause via LATERAL —
# there is no DataFrame equivalent that unnests a VARIANT array in one step, which is why
# this uses spark.sql like the other child tables.
# =====================================================================================
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_talent_workspace_history",
    comment="One row per workspace membership period of a talent, from the source's history[] array.",
    table_properties={"quality": "silver"},
    cluster_by=["talent_id"],
)
@dp.expect_all_or_drop(WORKSPACE_HISTORY_MUST_HOLD)
@dp.expect_all(WORKSPACE_HISTORY_SHOULD_HOLD)
def whoz_talent_workspace_history():
    return spark.sql(f"""
        SELECT
            b.talent_id,
            h.pos                                             AS ordinal,
            try_variant_get(h.value, '$.workspaceId','string') AS workspace_id,
            try_variant_get(h.value, '$.scope',      'string') AS scope,
            -- keep BOTH the parsed date and the raw string, the same way positions[] does:
            -- the parsed one is NULL for any value the cast can't handle, and you want to
            -- be able to see which.
            try_variant_get(h.value, '$.since',      'date')   AS since_date,
            try_variant_get(h.value, '$.since',      'string') AS since_raw,
            b.ingested_at
        FROM STREAM({BRONZE_TABLE}) AS b,
             LATERAL variant_explode(b.payload:history) AS h
    """)

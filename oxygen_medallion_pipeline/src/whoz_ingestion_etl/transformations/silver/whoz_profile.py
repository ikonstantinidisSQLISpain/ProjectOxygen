# =====================================================================================
# SILVER — silver.whoz_profiles / silver.whoz_profile_history
#
# Profile-level (1 row per profile) flattening of the bronze VARIANT payload.
# Child collections are handled in silver/whoz_profile_children.py.
#
# Every extraction uses try_variant_get so that a bad value nulls one column instead
# of failing the update. Collections stay as VARIANT here — they are unnested in the
# child tables.
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# The shared code lives in the sibling src/whoz_ingestion/ package, not under this
# pipeline folder: the pipeline's root_path IS src/ (see
# resources/whoz_ingestion_etl.pipeline.yml), so src/ itself is on sys.path at runtime and
# `whoz_ingestion.x` resolves. tests/ import by this same path (see pyproject.toml's
# pythonpath) so a wrong prefix here fails the test suite too, not only at deploy time.
from whoz_ingestion.expectations import PROFILE_MUST_HOLD, PROFILE_SHOULD_HOLD
from whoz_ingestion.shaping.profile import PROFILE_COLUMNS, PROFILE_HISTORY_COLUMNS, shape_profile

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles"


# -------------------------------------------------------------------------------------
# Shaped, validated rows off bronze — pipeline-scoped, materializes nothing itself.
# Whoz re-lands a full snapshot under each dated export (see bronze/whoz_profiles.py),
# so the same profile_id shows up once per snapshot here. Both AUTO CDC flows below
# read this same view and turn that into an upsert, keyed by profile_id. A temporary
# view is never a catalog object, so it keeps its bare name — nothing to qualify.
# -------------------------------------------------------------------------------------
# Both rule sets are defined in whoz_ingestion/expectations.py, not inline here: that module
# imports nothing, so tests/layer3_rules/test_profile_rules.py can import it and evaluate
# every predicate against real shape_profile() output. A rule naming a column that does not
# exist then fails in pytest instead of passing validate and firing on nothing forever.
@dp.temporary_view
@dp.expect_all_or_drop(PROFILE_MUST_HOLD)
@dp.expect_all(PROFILE_SHOULD_HOLD)
def whoz_profile_shaped():
    return shape_profile(spark.readStream.table(BRONZE_TABLE))


# whoz_profiles — SCD Type 1: one row per profile_id, current state only. Each new
# snapshot upserts in place; the previous version of a changed profile is gone.
dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profiles",
    comment=(
        "One row per Whoz profile, current state only — see whoz_profile_history for prior "
        "versions. No 'payload' column: query bronze.whoz_profiles by profile_id for the raw JSON."
    ),
    schema=PROFILE_COLUMNS,
    table_properties={"quality": "silver"},
    cluster_by=["federation_id", "profile_id"],
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profiles",
    source="whoz_profile_shaped",
    keys=["profile_id"],
    # Whoz's own last-modified time on the record, not when we happened to ingest it —
    # protects against an older dated export landing after a newer one (e.g. backfill).
    sequence_by=F.col("source_last_modified_at"),
    stored_as_scd_type="1",
    # No except_column_list: the source view no longer carries a payload column at all
    # (see shape_profile). It used to, and had to be excluded here, because AUTO CDC
    # compares whole rows with `<=>` to detect real changes and VARIANT does not support
    # that comparison — it failed with INVALID_ORDERING_TYPE. Not selecting the column
    # in the first place solves the same problem and saves reading it.
)

# whoz_profile_history — SCD Type 2: every version of every profile, each row valid
# for __START_AT to __END_AT (NULL __END_AT = still current). Same keys/ordering as
# whoz_profiles above; only the storage type differs.
dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_history",
    comment=(
        "Full version history of whoz_profiles, one row per (profile_id, version). "
        "__START_AT/__END_AT mark each version's validity window; NULL __END_AT is current. "
        "No 'payload' column: query bronze.whoz_profiles by profile_id for the raw JSON."
    ),
    # SCD2 requires __START_AT/__END_AT in an explicit schema, typed to match sequence_by
    # (source_last_modified_at, TIMESTAMP). Both the concatenation and that type rule live
    # in PROFILE_HISTORY_COLUMNS so tests/layer2_contract/test_profile_contract.py checks
    # this exact string.
    schema=PROFILE_HISTORY_COLUMNS,
    table_properties={"quality": "silver"},
    cluster_by=["profile_id"],
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_history",
    source="whoz_profile_shaped",
    keys=["profile_id"],
    sequence_by=F.col("source_last_modified_at"),
    stored_as_scd_type="2",
    # See the note on whoz_profiles' flow above. This SCD2 flow is the one that actually
    # hit the VARIANT comparison failure, since version-boundary detection diffs
    # consecutive rows for a profile_id; it is fixed at the source now, not excluded.
)


# =====================================================================================
# SILVER — silver.whoz_profile_completion_rules
#
# completionDetails is a MAP keyed by rule name, NOT a struct — and it arrives as an
# empty ARRAY [] on 723 of the 4,113 records. Exploding it to one row per
# (profile, rule) means a new scoring rule from Whoz shows up as new *rows*, not as a
# schema change, and the array-vs-object polymorphism is handled by the filter below.
# =====================================================================================
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_completion_rules",
    comment="One row per (profile, completion rule) — unpacked from the completionDetails map.",
    table_properties={"quality": "silver"},
)
def whoz_profile_completion_rules():
    return spark.sql(f"""
        SELECT
            b.profile_id,
            b.talent_id,
            e.key                                                  AS rule_name,
            try_variant_get(e.value, '$.satisfied', 'boolean')      AS is_satisfied,
            try_variant_get(e.value, '$.weight',    'int')          AS weight,
            b.ingested_at
        FROM STREAM({BRONZE_TABLE}) AS b,
             LATERAL variant_explode(b.payload:completionDetails) AS e
        -- variant_explode on an object yields key/value; on the empty-array form it
        -- yields no rows at all, which is exactly what we want.
        WHERE e.key IS NOT NULL
    """)

# =====================================================================================
# SILVER — whoz_profile
#
# Profile-level (1 row per profile) flattening of the bronze VARIANT payload.
# Child collections are handled in silver_whoz_profile_children.py.
#
# Every extraction uses try_variant_get so that a bad value nulls one column instead
# of failing the update. Collections stay as VARIANT here — they are unnested in the
# child tables.
# =====================================================================================

from pyspark import pipelines as dp

from whoz_ingestion_etl.utilities.profile_shaping import shape_profile


@dp.table(
    name="whoz_profile",
    comment="One row per Whoz profile, scalar fields unpacked from the bronze VARIANT payload.",
    table_properties={"quality": "silver"},
    cluster_by=["federation_id", "profile_id"],
)
@dp.expect_or_drop("profile_id_not_null", "profile_id IS NOT NULL")
@dp.expect("talent_id_not_null", "talent_id IS NOT NULL")
# The source is unversioned in this extract (main=true everywhere), but the model
# allows several versions per talent — this warns if that ever starts happening.
@dp.expect("is_main_version", "is_main_version = true")
def whoz_profile():
    return shape_profile(spark.readStream.table("whoz_profiles_bronze"))


# =====================================================================================
# SILVER — whoz_profile_completion_rule
#
# completionDetails is a MAP keyed by rule name, NOT a struct — and it arrives as an
# empty ARRAY [] on 723 of the 4,113 records. Exploding it to one row per
# (profile, rule) means a new scoring rule from Whoz shows up as new *rows*, not as a
# schema change, and the array-vs-object polymorphism is handled by the filter below.
# =====================================================================================
@dp.table(
    name="whoz_profile_completion_rule",
    comment="One row per (profile, completion rule) — unpacked from the completionDetails map.",
    table_properties={"quality": "silver"},
)
def whoz_profile_completion_rule():
    return spark.sql("""
        SELECT
            b.profile_id,
            b.talent_id,
            e.key                                                  AS rule_name,
            try_variant_get(e.value, '$.satisfied', 'boolean')      AS is_satisfied,
            try_variant_get(e.value, '$.weight',    'int')          AS weight,
            b.ingested_at
        FROM STREAM(whoz_profiles_bronze) AS b,
             LATERAL variant_explode(b.payload:completionDetails) AS e
        -- variant_explode on an object yields key/value; on the empty-array form it
        -- yields no rows at all, which is exactly what we want.
        WHERE e.key IS NOT NULL
    """)

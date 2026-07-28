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
from pyspark.sql import functions as F


def vg(path, target_type):
    """try_variant_get on the payload column — NULL on missing path or bad cast."""
    return F.try_variant_get("payload", path, target_type)


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
    bronze = spark.readStream.table("whoz_profiles_bronze")

    return bronze.select(
        # ---- identity ----
        F.col("profile_id"),
        F.col("talent_id"),
        F.col("federation_id"),
        vg("$.versionName", "string").alias("version_name"),
        vg("$.main", "boolean").alias("is_main_version"),
        # ---- classification ----
        vg("$.status", "string").alias("status"),                  # DRAFT|VALIDATED|SUBMITTED
        vg("$.contentLanguage", "string").alias("content_language"),
        vg("$.permissionScope", "string").alias("permission_scope"),
        vg("$.travelRange", "string").alias("travel_range"),
        vg("$.removed", "boolean").alias("is_removed"),
        vg("$.resumeRelationStatus", "string").alias("resume_relation_status"),
        # ---- completeness scoring ----
        # int on some records, float on others -> always read as double
        vg("$.completionRate", "double").alias("completion_rate"),
        vg("$.completionRateLastComputedDate", "timestamp").alias("completion_rate_computed_at"),
        # ---- headline (1:1 embedded object; absent on ~25% of records) ----
        vg("$.headline.jobTitle", "string").alias("headline_job_title"),
        vg("$.headline.seekingOpportunities", "boolean").alias("seeking_opportunities"),
        vg("$.headline.seekingOpportunitiesLastModifiedDate", "timestamp")
        .alias("seeking_opportunities_updated_at"),
        vg("$.headline.permissionScope", "string").alias("headline_permission_scope"),
        # Fields below are null on 100% of records today. Kept so the column exists
        # the day Whoz starts populating them — costs nothing in Delta.
        vg("$.headline.aim", "string").alias("headline_aim"),
        vg("$.headline.nationalMobility", "string").alias("national_mobility"),
        vg("$.headline.internationalMobility", "string").alias("international_mobility"),
        vg("$.headline.mobilityDate", "string").alias("mobility_date_raw"),
        vg("$.headline.mobilityNote", "string").alias("mobility_note"),
        # ---- free text ----
        vg("$.hobbies", "string").alias("hobbies"),
        # ---- audit fields from the source system ----
        # Timestamps arrive at second, millisecond and nanosecond precision — casting
        # to timestamp handles all three; a fixed format string would not.
        vg("$.createdDate", "timestamp").alias("source_created_at"),
        vg("$.createdBy", "string").alias("source_created_by"),
        vg("$.lastModifiedDate", "timestamp").alias("source_last_modified_at"),
        vg("$.lastModifiedBy", "string").alias("source_last_modified_by"),
        vg("$.lastExplicitUpdate", "timestamp").alias("source_last_explicit_update_at"),
        vg("$.lastExplicitUpdateBy", "string").alias("source_last_explicit_update_by"),
        # ---- collection sizes: cheap, and they make quality drift obvious ----
        F.expr("try_cast(size(cast(payload:aptitudes as array<variant>)) as int)")
        .alias("aptitude_count"),
        F.expr("try_cast(size(cast(payload:positions as array<variant>)) as int)")
        .alias("position_count"),
        F.expr("try_cast(size(cast(payload:skillRatings as array<variant>)) as int)")
        .alias("skill_rating_count"),
        F.expr("try_cast(size(cast(payload:qualificationIds as array<variant>)) as int)")
        .alias("qualification_count"),
        # ---- keep the raw payload so nothing modelled later is lost ----
        F.col("payload"),
        # ---- lineage ----
        F.col("source_file"),
        F.col("ingested_at"),
    )


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

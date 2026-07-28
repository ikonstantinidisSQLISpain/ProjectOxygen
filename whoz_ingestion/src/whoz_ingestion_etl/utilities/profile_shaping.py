# =====================================================================================
# Pure transformation logic for the whoz_profile silver table.
#
# Deliberately has NO dependency on `pyspark.pipelines` (@dp.table etc). That module
# only exists inside a running Lakeflow pipeline — importing it locally or in CI
# raises ImportError, so any test that imports a pipeline-decorated file transitively
# fails to even collect. Keeping shaping logic here, with only plain pyspark.sql
# imports, is what makes it unit-testable with a plain SparkSession.
#
# silver_whoz_profile.py imports shape_profile() and wraps it with @dp.table.
# =====================================================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def vg(path, target_type):
    """try_variant_get on the payload column — NULL on missing path or bad cast."""
    return F.try_variant_get("payload", path, target_type)


def shape_profile(bronze: DataFrame) -> DataFrame:
    """Bronze columns -> whoz_profile columns."""
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

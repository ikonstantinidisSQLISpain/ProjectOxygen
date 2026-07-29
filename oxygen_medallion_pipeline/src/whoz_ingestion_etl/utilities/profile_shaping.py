# =====================================================================================
# Pure transformation logic for the whoz_profile silver table.
#
# Deliberately has NO dependency on `pyspark.pipelines` (@dp.table etc). That module
# only exists inside a running Lakeflow pipeline — importing it locally or in CI
# raises ImportError, so any test that imports a pipeline-decorated file transitively
# fails to even collect. Keeping shaping logic here, with only plain pyspark.sql
# imports, is what makes it unit-testable with a plain SparkSession.
#
# silver_whoz_profile.py imports shape_profile() and wraps it with @dp.table, and
# imports PROFILE_COLUMNS as the declared schema of the tables it writes.
# =====================================================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# The declared schema of silver.whoz_profiles / silver.whoz_profile_history. It lives
# here, next to shape_profile(), because the two must agree column-for-column and
# type-for-type — keeping them in one file means a change to one puts the other right
# under your eyes. It is also why this constant is here rather than in
# silver_whoz_profile.py: that module imports pyspark.pipelines and so cannot be
# imported by a test, while this one can, which is what lets
# tests/test_silver_whoz_profile.py both parse this DDL and diff it against
# shape_profile()'s real output. A raw SQL string is unavoidable (it is what
# create_streaming_table's schema= takes) and is unforgiving: a bare apostrophe inside
# a COMMENT ends the string literal early and the whole schema fails to parse, which
# no amount of py_compile / bundle validate will notice. Escape one by doubling it
# ('Whoz''s'), and trust the tests to catch it if you forget.
#
# Matches shape_profile()'s SELECT exactly, column for column and in order — the test
# asserts precisely that. Distributions named here ("false on every record today")
# describe the current export, not a guarantee — see docs/whoz_profile_data_model.md.
PROFILE_COLUMNS = """
    profile_id STRING NOT NULL COMMENT 'Whoz profile ID, stable primary key. NOT NULL is enforced upstream by profile_id_not_null (expect_or_drop)',
    talent_id STRING COMMENT 'One profile per talent today; the model allows several versions per talent',
    federation_id STRING COMMENT 'Tenant identifier; a single value across this whole export (single-tenant)',
    version_name STRING COMMENT '"Main version" on every record today',
    is_main_version BOOLEAN COMMENT 'true on every record today; is_main_version expectation upstream warns if that ever changes',
    status STRING COMMENT 'DRAFT | VALIDATED | SUBMITTED',
    content_language STRING COMMENT 'en / fr / nl / it / de / es, per docs/whoz_profile_data_model.md',
    permission_scope STRING COMMENT 'SECRET on every record today',
    travel_range STRING COMMENT 'DEFAULT on every record today',
    is_removed BOOLEAN COMMENT 'false on every record today',
    resume_relation_status STRING COMMENT 'Includes a typo in the source enum: RESUME_IMPORT_SUGGESTION_SUBMITED',
    completion_rate DOUBLE COMMENT 'Profile completeness score, 0-1 (i.e. 0.42 = 42% complete). Computed by Whoz, not by us, from the weights assigned to the completion rules — see silver.whoz_profile_completion_rules. Source sends int on some records and float on others; always read as double',
    completion_rate_computed_at TIMESTAMP COMMENT 'When completion_rate was last computed',
    headline_job_title STRING COMMENT 'From the embedded headline object, absent on roughly a quarter of profiles',
    seeking_opportunities BOOLEAN COMMENT 'Non-null on very few profiles today',
    seeking_opportunities_updated_at TIMESTAMP,
    headline_permission_scope STRING COMMENT 'SECRET on every record today',
    headline_aim STRING COMMENT 'Null on every record today; kept so the column exists once Whoz starts populating it',
    national_mobility STRING COMMENT 'Null on every record today; kept so the column exists once Whoz starts populating it',
    international_mobility STRING COMMENT 'Null on every record today; kept so the column exists once Whoz starts populating it',
    mobility_date_raw STRING COMMENT 'Null on every record today; kept so the column exists once Whoz starts populating it',
    mobility_note STRING COMMENT 'Null on every record today; kept so the column exists once Whoz starts populating it',
    hobbies STRING COMMENT 'Free text',
    source_created_at TIMESTAMP COMMENT 'When Whoz created this profile record',
    source_created_by STRING COMMENT 'Whoz user ObjectId',
    source_last_modified_at TIMESTAMP COMMENT 'Whoz''s own last-modified time on the record — what AUTO CDC sequences by, not our ingest time',
    source_last_modified_by STRING,
    source_last_explicit_update_at TIMESTAMP,
    source_last_explicit_update_by STRING,
    aptitude_count INT COMMENT 'size(aptitudes[]) at the source; exploded rows live in silver.whoz_profile_aptitudes',
    position_count INT COMMENT 'size(positions[]) at the source; exploded rows live in silver.whoz_profile_positions',
    skill_rating_count INT COMMENT 'size(skillRatings[]) at the source; legacy, mostly zero, see silver.whoz_profile_skill_ratings',
    qualification_count INT COMMENT 'size(qualificationIds[]) at the source; too thin (423 values total) to model as its own table',
    source_file STRING COMMENT 'Bronze lineage: which landed file this profile version came from',
    ingested_at TIMESTAMP COMMENT 'Bronze lineage: when this snapshot was ingested, not when Whoz generated it'
"""


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
        # No payload column in the OUTPUT. Bronze keeps the full raw VARIANT forever
        # (join back on profile_id) and the child tables explode it straight off bronze,
        # so nothing needs it here: both AUTO CDC flows discarded it anyway, and it is
        # what made them fail on VARIANT comparison in the first place.
        #
        # Note this does not avoid *reading* payload — every column above is derived
        # from it, so Delta reads it regardless. What it avoids is carrying the single
        # largest column of the dataset through the view and into the AUTO CDC merge
        # only to drop it there.
        # ---- lineage ----
        F.col("source_file"),
        F.col("ingested_at"),
    )

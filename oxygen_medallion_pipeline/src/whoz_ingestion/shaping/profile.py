# =====================================================================================
# Pure transformation logic for the whoz_profile silver table.
#
# Deliberately has NO dependency on `pyspark.pipelines` (@dp.table etc). That module
# only exists inside a running Lakeflow pipeline — importing it locally or in CI
# raises ImportError, so any test that imports a pipeline-decorated file transitively
# fails to even collect. Keeping shaping logic here, with only plain pyspark.sql
# imports, is what makes it unit-testable with a plain SparkSession.
#
# silver/whoz_profile.py imports shape_profile() and wraps it with @dp.table, and
# imports PROFILE_COLUMNS as the declared schema of the tables it writes.
# =====================================================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from whoz_ingestion.contract import SCD2_COLUMNS, ddl, load_columns
from whoz_ingestion.shaping import collection_size_sql

# The declared schema of silver.whoz_profiles / silver.whoz_profile_history. The columns
# themselves are data, in ../schemas/whoz_profile.yml; contract.ddl() renders them into the
# string create_streaming_table's schema= takes. This constant lives here, next to
# shape_profile(), because the two must agree column-for-column and type-for-type — the
# schema file is a file away, and it is the SELECT below that has to be kept in step with
# it. It is also why the constant is here rather than in silver/whoz_profile.py: that module
# imports pyspark.pipelines and so cannot be imported by a test, while this one can, which
# is what lets tests/layer2_contract/test_profile_contract.py both parse this DDL and diff
# it against shape_profile()'s real output.
PROFILE_COLUMN_DEFS = load_columns("whoz_profile")
PROFILE_COLUMNS = ddl(PROFILE_COLUMN_DEFS)

# The schema of silver.whoz_profile_history: the same columns plus AUTO CDC's SCD2 validity
# window. Built here rather than at the call site in silver/whoz_profile.py so that
# tests/layer2_contract/test_profile_contract.py checks the real string the pipeline uses,
# not a copy of it. Why both SCD2 columns must be TIMESTAMP is recorded on SCD2_COLUMNS.
PROFILE_HISTORY_COLUMNS = ddl(PROFILE_COLUMN_DEFS + SCD2_COLUMNS)


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
        vg("$.status", "string").alias("status"),  # DRAFT|VALIDATED|SUBMITTED
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
        vg("$.headline.seekingOpportunitiesLastModifiedDate", "timestamp").alias("seeking_opportunities_updated_at"),
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
        # NULL, not -1, when the key is absent: `size(NULL)` is -1 on Databricks and NULL on
        # the local test engine, so these went through collection_size_sql after a deployed
        # run showed the difference. See its docstring.
        F.expr(collection_size_sql("try_variant_get(payload, '$.aptitudes', 'array<variant>')")).alias(
            "aptitude_count"
        ),
        F.expr(collection_size_sql("try_variant_get(payload, '$.positions', 'array<variant>')")).alias(
            "position_count"
        ),
        F.expr(collection_size_sql("try_variant_get(payload, '$.skillRatings', 'array<variant>')")).alias(
            "skill_rating_count"
        ),
        F.expr(collection_size_sql("try_variant_get(payload, '$.qualificationIds', 'array<variant>')")).alias(
            "qualification_count"
        ),
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

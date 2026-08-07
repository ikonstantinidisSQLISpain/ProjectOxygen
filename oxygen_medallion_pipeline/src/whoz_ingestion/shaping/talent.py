# =====================================================================================
# Pure transformation logic for the whoz_talent silver table.
#
# Same contract as shaping/profile.py: NO dependency on `pyspark.pipelines`, plain
# DataFrame in / DataFrame out, so it can be unit tested against a local SparkSession.
# silver/whoz_talent.py imports shape_talent() and wraps it with @dp.table.
#
# WHAT THIS DOES AND DOESN'T MODEL
#
# The talent export nests a whole profile object under `profile`. That object overlaps
# heavily with the separate profile export already modelled in shaping/profile.py, but is
# not identical — it carries educations[], links[], mainSkills[], secondarySkills[],
# unclassifiedSkills[] and headline.bio / headline.company, none of which appear in the
# profile export.
#
# This module deliberately does NOT re-model the embedded profile. It lifts the profile's
# key and a few cheap attributes as denormalized columns, and leaves the profile itself to
# silver.whoz_profiles, joined on profile_id. Reasons:
#
#   * one source of truth per entity — two pipelines writing overlapping profile columns
#     from two exports is a reconciliation problem nobody wants
#   * the raw nested object is still in bronze.whoz_talents' VARIANT payload forever, so
#     nothing is lost and this decision is reversible without a re-ingest
#   * it keeps the join key and the integrity checks (embedded_talent_id_agrees) visible
#     as ordinary columns, which is what makes them testable
#
# If Whoz ever retires the standalone profile export, the extra fields above are what a
# replacement would need to cover.
# =====================================================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from whoz_ingestion.contract import SCD2_COLUMNS, ddl, load_columns

# The declared schema of silver.whoz_talents / silver.whoz_talent_versions. Same shape as
# PROFILE_COLUMNS in shaping/profile.py: the columns are data in ../schemas/whoz_talent.yml,
# contract.ddl() renders them, and they must match shape_talent()'s SELECT column for column
# and in order — tests/layer2_contract/test_talent_contract.py asserts exactly that.
TALENT_COLUMN_DEFS = load_columns("whoz_talent")
TALENT_COLUMNS = ddl(TALENT_COLUMN_DEFS)

# The schema of silver.whoz_talent_versions: the same columns plus AUTO CDC's SCD2 validity
# window. Built here rather than at the call site so
# tests/layer2_contract/test_talent_contract.py checks the real string the pipeline uses.
TALENT_HISTORY_COLUMNS = ddl(TALENT_COLUMN_DEFS + SCD2_COLUMNS)


def vg(path, target_type):
    """try_variant_get on the payload column — NULL on missing path or bad cast."""
    return F.try_variant_get("payload", path, target_type)


def size_of(path):
    """size() of a payload collection as an INT, NULL rather than an error if it isn't one.

    Note NULL, not 0, when the key is absent — the same behaviour as the *_count columns in
    shaping/profile.py, and worth knowing before summing one of these downstream.
    """
    return F.expr(f"try_cast(size(cast(payload:{path} as array<variant>)) as int)")


def shape_talent(bronze: DataFrame) -> DataFrame:
    """Bronze columns -> whoz_talent columns."""
    return bronze.select(
        # ---- identity (already lifted out of the payload by bronze) ----
        F.col("talent_id"),
        F.col("federation_id"),
        F.col("user_id"),
        F.col("workspace_id"),
        # ---- talent attributes ----
        vg("$.permissionScope", "string").alias("permission_scope"),
        vg("$.removed", "boolean").alias("is_removed"),
        vg("$.endUser", "boolean").alias("is_end_user"),
        vg("$.freelyAssignable", "boolean").alias("is_freely_assignable"),
        vg("$.timeEntryPreferredUnit", "string").alias("time_entry_preferred_unit"),
        # Timestamps arrive at second, millisecond and nanosecond precision — casting to
        # timestamp handles all three; a fixed format string would not.
        vg("$.lastConnectionDate", "timestamp").alias("last_connection_at"),
        # ---- the embedded profile: key + light attributes only, see the module header ----
        vg("$.profile.id", "string").alias("profile_id"),
        vg("$.profile.talentId", "string").alias("profile_talent_id"),
        vg("$.profile.versionName", "string").alias("profile_version_name"),
        vg("$.profile.main", "boolean").alias("profile_is_main_version"),
        vg("$.profile.status", "string").alias("profile_status"),
        # int on some records and float on others -> always read as double
        vg("$.profile.completionRate", "double").alias("profile_completion_rate"),
        # The drift sensor that makes the 1:1 assumption above falsifiable. schema_of_variant
        # returns the full logical type of a VARIANT value — "OBJECT<aptitudes: ARRAY<VOID>,
        # ...>" for one profile, "ARRAY<OBJECT<...>>" for several — so the leading token is
        # all we need, and keeping just that token stops a 2 KB type string landing in a
        # column. Verified against both forms before writing this.
        #
        # This matters because the array form does not fail: every vg("$.profile.*") above
        # silently returns NULL, the pipeline stays green, and silver fills with talents that
        # appear to have no profile. See the profile_is_not_an_array DQX check.
        F.regexp_extract(F.expr("schema_of_variant(payload:profile)"), r"^([A-Za-z]+)", 1)
        .alias("profile_container_type"),
        # ---- audit fields from the source system ----
        vg("$.createdDate", "timestamp").alias("source_created_at"),
        vg("$.createdBy", "string").alias("source_created_by"),
        vg("$.lastModifiedDate", "timestamp").alias("source_last_modified_at"),
        vg("$.lastModifiedBy", "string").alias("source_last_modified_by"),
        # ---- collection sizes: cheap, and they make quality drift obvious ----
        # Most of these are empty on every record today. They are here as sensors: the day
        # one starts filling up, it shows as a number moving rather than as a silent gap.
        size_of("history").alias("workspace_history_count"),
        size_of("tags").alias("tag_count"),
        size_of("aspirations").alias("aspiration_count"),
        size_of("maxWorkingHours").alias("max_working_hours_count"),
        size_of("sharingDestinations").alias("sharing_destination_count"),
        size_of("qualificationIds").alias("qualification_count"),
        size_of("customFields").alias("custom_field_count"),
        size_of("recruitment.stageDates").alias("recruitment_stage_date_count"),
        size_of("recruitment.workflowStepDates").alias("recruitment_workflow_step_date_count"),
        # No payload column in the OUTPUT — same reason as shape_profile: AUTO CDC compares
        # whole rows with `<=>` to detect real changes and VARIANT does not support that
        # comparison (INVALID_ORDERING_TYPE). Bronze keeps the raw payload forever; join
        # back on talent_id.
        # ---- lineage ----
        F.col("source_file"),
        F.col("ingested_at"),
    )

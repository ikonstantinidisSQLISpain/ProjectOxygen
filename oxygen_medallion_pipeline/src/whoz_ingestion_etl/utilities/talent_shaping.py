# =====================================================================================
# Pure transformation logic for the whoz_talent silver table.
#
# Same contract as profile_shaping.py: NO dependency on `pyspark.pipelines`, plain
# DataFrame in / DataFrame out, so it can be unit tested against a local SparkSession.
# silver_whoz_talent.py imports shape_talent() and wraps it with @dp.table.
#
# WHAT THIS DOES AND DOESN'T MODEL
#
# The talent export nests a whole profile object under `profile`. That object overlaps
# heavily with the separate profile export already modelled in profile_shaping.py, but is
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

# The declared schema of silver.whoz_talents / silver.whoz_talent_versions. Same rules as
# PROFILE_COLUMNS in profile_shaping.py: it must match shape_talent()'s SELECT column for
# column and in order (tests/test_schema_contract.py asserts exactly that), and an
# unescaped apostrophe inside a COMMENT ends the string literal early and takes the whole
# schema down with it — double it ('Whoz''s') and trust the tests to catch it if you forget.
#
# Distributions named here describe the export analysed on 2026-07-31, not a guarantee.
TALENT_COLUMNS = """
    talent_id STRING NOT NULL COMMENT 'Whoz talent ID, stable primary key. NOT NULL is enforced upstream by talent_pk_not_null (expect_or_drop)',
    federation_id STRING COMMENT 'Tenant identifier; a single value across this whole export (single-tenant)',
    user_id STRING COMMENT 'Whoz user ObjectId behind this talent',
    workspace_id STRING COMMENT 'Workspace the talent currently belongs to; see silver.whoz_talent_workspace_history for the full membership history',
    permission_scope STRING COMMENT 'SECRET on every record today',
    is_removed BOOLEAN COMMENT 'Soft-delete flag at the source',
    is_end_user BOOLEAN,
    is_freely_assignable BOOLEAN COMMENT 'Staffing flag; false on every record today',
    time_entry_preferred_unit STRING COMMENT 'DAY | HOUR',
    last_connection_at TIMESTAMP COMMENT 'Last time the talent logged in to Whoz. Null for a talent who never has',
    profile_id STRING COMMENT 'Foreign key to silver.whoz_profiles. Lifted out of the embedded profile object — the profile itself is modelled from the separate profile export, not here',
    profile_talent_id STRING COMMENT 'talentId as repeated inside the embedded profile object; redundant with talent_id and kept only as an integrity check (see embedded_talent_id_agrees)',
    profile_version_name STRING COMMENT '"Main version" on every record today',
    profile_is_main_version BOOLEAN,
    profile_status STRING COMMENT 'DRAFT | VALIDATED | SUBMITTED',
    profile_completion_rate DOUBLE COMMENT 'Profile completeness score, 0-1 (0.42 = 42% complete). Source sends int on some records and float on others; always read as double',
    profile_container_type STRING COMMENT 'Drift sensor: the VARIANT container type of the embedded profile — OBJECT while one talent has one profile, ARRAY the day Whoz starts sending several. Watched by the profile_is_not_an_array expectation, because the array form silently NULLs every profile_* column above rather than failing',
    source_created_at TIMESTAMP COMMENT 'When Whoz created this talent record',
    source_created_by STRING COMMENT 'Whoz user ObjectId',
    source_last_modified_at TIMESTAMP COMMENT 'Whoz''s own last-modified time on the record — what AUTO CDC sequences by, not our ingest time',
    source_last_modified_by STRING,
    workspace_history_count INT COMMENT 'size(history[]) at the source; exploded rows live in silver.whoz_talent_workspace_history',
    tag_count INT COMMENT 'size(tags[]) at the source; empty on every record today',
    aspiration_count INT COMMENT 'size(aspirations[]) at the source; empty on every record today',
    max_working_hours_count INT COMMENT 'size(maxWorkingHours[]) at the source; empty on every record today',
    sharing_destination_count INT COMMENT 'size(sharingDestinations[]) at the source; empty on every record today',
    qualification_count INT COMMENT 'size(qualificationIds[]) at the source',
    custom_field_count INT COMMENT 'size(customFields[]) at the source; empty on every record today',
    recruitment_stage_date_count INT COMMENT 'size(recruitment.stageDates[]) at the source; empty on every record today',
    recruitment_workflow_step_date_count INT COMMENT 'size(recruitment.workflowStepDates[]) at the source; empty on every record today',
    source_file STRING COMMENT 'Bronze lineage: which landed file this talent version came from',
    ingested_at TIMESTAMP COMMENT 'Bronze lineage: when this snapshot was ingested, not when Whoz generated it'
"""


# The schema of silver.whoz_talent_versions: the same columns plus AUTO CDC's SCD2 validity
# window. Built here rather than concatenated at the call site so tests/test_schema_contract.py
# checks the real string the pipeline uses. Both columns must be TIMESTAMP to match the
# flow's sequence_by (source_last_modified_at).
TALENT_HISTORY_COLUMNS = (
    TALENT_COLUMNS
    + """,
    __START_AT TIMESTAMP COMMENT 'Start of this version''s validity window (SCD2, added by AUTO CDC)',
    __END_AT TIMESTAMP COMMENT 'End of this version''s validity window; NULL means still current (SCD2, added by AUTO CDC)'
"""
)


def vg(path, target_type):
    """try_variant_get on the payload column — NULL on missing path or bad cast."""
    return F.try_variant_get("payload", path, target_type)


def size_of(path):
    """size() of a payload collection as an INT, NULL rather than an error if it isn't one.

    Note NULL, not 0, when the key is absent — the same behaviour as the *_count columns in
    profile_shaping.py, and worth knowing before summing one of these downstream.
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
        # appear to have no profile. See the profile_is_not_an_array expectation.
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

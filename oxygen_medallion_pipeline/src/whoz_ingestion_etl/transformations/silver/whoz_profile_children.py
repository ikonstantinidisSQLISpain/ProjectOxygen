# =====================================================================================
# SILVER — child tables exploded out of the bronze VARIANT payload
#
#   whoz_profile_aptitudes       113,810 rows   PK aptitude_id
#   whoz_profile_positions        15,104 rows   PK position_id (self-hierarchical)
#   whoz_position_aptitude_refs  137,050 rows   PK (position_id, aptitude_id)
#   whoz_profile_skill_ratings     6,792 rows   legacy — see docs/whoz_profile_data_model.md
#
# variant_explode is a table-valued generator, so it goes in the FROM clause via
# LATERAL. That is why these use spark.sql rather than the DataFrame API — there is
# no DataFrame equivalent that unnests a VARIANT array in one step.
# =====================================================================================

from pyspark import pipelines as dp

# Quality rules are defined as data in utilities/expectations.py rather than inline in the
# decorators below, so that tests/layer3_rules/test_rule_hygiene.py can check them — see
# that module's header for the reasoning. Hygiene is all these child-table rules get: their
# SQL bodies are inline below rather than in utilities/, so there is no local DataFrame to
# resolve their predicates against, and they have no per-entity behaviour file.
from utilities.expectations import (
    APTITUDE_MUST_HOLD,
    APTITUDE_REF_MUST_HOLD,
    APTITUDE_SHOULD_HOLD,
    POSITION_MUST_HOLD,
    POSITION_SHOULD_HOLD,
)

# See bronze/whoz_profiles.py — resources/whoz_ingestion_etl.pipeline.yml is the only
# source of truth for these, so no fallback value here.
CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles"


# -------------------------------------------------------------------------------------
# APTITUDES — the skills inventory. 2,134 of 4,113 profiles have any; max 373 per profile.
# -------------------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_aptitudes",
    comment="One row per aptitude (skill / language / tool) declared on a profile.",
    table_properties={"quality": "silver"},
    cluster_by=["profile_id", "aptitude_type"],
)
@dp.expect_all_or_drop(APTITUDE_MUST_HOLD)
@dp.expect_all(APTITUDE_SHOULD_HOLD)
def whoz_profile_aptitudes():
    return spark.sql(f"""
        SELECT
            try_variant_get(a.value, '$.id',        'string')  AS aptitude_id,
            b.profile_id,
            b.talent_id,
            a.pos                                              AS ordinal,
            try_variant_get(a.value, '$.name',      'string')  AS aptitude_name,
            try_variant_get(a.value, '$.type',      'string')  AS aptitude_type,
            -- conceptId is the taxonomy key: 6,626 distinct concepts behind 13,222
            -- distinct free-text names. NULL on ~2% = unmapped free-text skill.
            try_variant_get(a.value, '$.conceptId', 'string')  AS concept_id,
            try_variant_get(a.value, '$.visibility','string')  AS visibility,
            try_variant_get(a.value, '$.proficiency','int')    AS proficiency,
            -- int on 32,650 rows and float on 80,704 -> always double
            try_variant_get(a.value, '$.cumulativeExperience', 'double')
                                                               AS cumulative_experience_years,
            try_variant_get(a.value, '$.augmentedWithAi','boolean') AS augmented_with_ai,
            -- these two are redundant with the parent but useful as an integrity check
            try_variant_get(a.value, '$.profileId', 'string')  AS embedded_profile_id,
            try_variant_get(a.value, '$.talentId',  'string')  AS embedded_talent_id,
            a.value                                            AS aptitude_payload,
            b.ingested_at
        FROM STREAM({BRONZE_TABLE}) AS b,
             LATERAL variant_explode(b.payload:aptitudes) AS a
    """)


# -------------------------------------------------------------------------------------
# POSITIONS — jobs and missions in one array, split by is_mission and linked by
# parent_position_id (all 1,197 parent references resolve within the file).
# -------------------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_positions",
    comment="One row per position (job or mission) held on a profile. Self-hierarchical.",
    table_properties={"quality": "silver"},
    cluster_by=["profile_id", "start_date"],
)
@dp.expect_all_or_drop(POSITION_MUST_HOLD)
@dp.expect_all(POSITION_SHOULD_HOLD)
def whoz_profile_positions():
    return spark.sql(f"""
        SELECT
            try_variant_get(p.value, '$.id',       'string')  AS position_id,
            b.profile_id,
            b.talent_id,
            p.pos                                             AS ordinal,
            try_variant_get(p.value, '$.parentPositionId','string') AS parent_position_id,
            try_variant_get(p.value, '$.isMission','boolean') AS is_mission,
            try_variant_get(p.value, '$.current',  'boolean') AS is_current,
            try_variant_get(p.value, '$.title',    'string')  AS title,
            try_variant_get(p.value, '$.companyName',  'string') AS company_name,
            try_variant_get(p.value, '$.employerName', 'string') AS employer_name,
            try_variant_get(p.value, '$.missionName',    'string') AS mission_name,
            try_variant_get(p.value, '$.missionContext', 'string') AS mission_context,
            try_variant_get(p.value, '$.description',    'string') AS description,
            -- keep BOTH the parsed date and the raw string: the parsed one is NULL for
            -- the extended-year value in the source, and you want to see which.
            try_variant_get(p.value, '$.startDate', 'date')   AS start_date,
            try_variant_get(p.value, '$.endDate',   'date')   AS end_date,
            try_variant_get(p.value, '$.startDate', 'string') AS start_date_raw,
            try_variant_get(p.value, '$.endDate',   'string') AS end_date_raw,
            try_cast(size(cast(p.value:aptitudeReferences as array<variant>)) as int)
                                                              AS aptitude_reference_count,
            p.value                                           AS position_payload,
            b.ingested_at
        FROM STREAM({BRONZE_TABLE}) AS b,
             LATERAL variant_explode(b.payload:positions) AS p
    """)


# -------------------------------------------------------------------------------------
# POSITION -> APTITUDE bridge. No natural id on the child, so the key is the pair.
# 137,050 references against 113,810 aptitudes: the same skill is claimed on several
# positions, which is the point of the table.
# -------------------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_position_aptitude_refs",
    comment="Bridge: skills claimed on each position. Grain = (position_id, aptitude_id).",
    table_properties={"quality": "silver"},
    cluster_by=["profile_id", "position_id"],
)
@dp.expect_all_or_drop(APTITUDE_REF_MUST_HOLD)
def whoz_position_aptitude_refs():
    return spark.sql(f"""
        SELECT
            b.profile_id,
            try_variant_get(p.value, '$.id', 'string')          AS position_id,
            try_variant_get(r.value, '$.aptitudeId', 'string')  AS aptitude_id,
            try_variant_get(r.value, '$.conceptId',  'string')  AS concept_id,
            try_variant_get(r.value, '$.name',       'string')  AS aptitude_name,
            try_variant_get(r.value, '$.type',       'string')  AS aptitude_type,
            r.pos                                               AS ordinal,
            b.ingested_at
        FROM STREAM({BRONZE_TABLE}) AS b,
             LATERAL variant_explode(b.payload:positions) AS p,
             LATERAL variant_explode(p.value:aptitudeReferences) AS r
    """)


# -------------------------------------------------------------------------------------
# SKILL RATINGS — 6,792 rows but only 83 profiles, and 6,557 of them are rating 0.
# Almost certainly superseded by aptitudes[].proficiency. Landed for completeness;
# confirm with Whoz before anyone builds a metric on it.
# -------------------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_skill_ratings",
    comment="Legacy skillRatings array. Sparse and mostly zero — verify before use.",
    table_properties={"quality": "silver"},
)
def whoz_profile_skill_ratings():
    return spark.sql(f"""
        SELECT
            b.profile_id,
            b.talent_id,
            s.pos                                        AS ordinal,
            try_variant_get(s.value, '$.skill',  'string') AS skill_name,
            try_variant_get(s.value, '$.rating', 'int')    AS rating,
            b.ingested_at
        FROM STREAM({BRONZE_TABLE}) AS b,
             LATERAL variant_explode(b.payload:skillRatings) AS s
    """)


# -------------------------------------------------------------------------------------
# Not modelled on purpose — empty on every one of the 4,113 records in this extract:
#   customFields, functionalDomains, schedules, targetFunctionalDomains,
#   targetSkillRatings, positions[].customFields, headline.mobilityDestinations
#
# Rather than build empty tables, watch for them filling up. Add this expectation to
# whoz_profiles and it will alert the first time Whoz starts sending any of them:
#
#   @dp.expect("unmodelled_collections_still_empty",
#              "try_cast(size(cast(payload:customFields as array<variant>)) as int) = 0 "
#              "AND try_cast(size(cast(payload:schedules as array<variant>)) as int) = 0")
#
# Same idea for qualificationIds[] and targetSkills[] — 423 and 2 values respectively,
# too thin to be worth a table today.
# -------------------------------------------------------------------------------------

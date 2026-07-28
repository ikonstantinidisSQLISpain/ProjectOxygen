# =====================================================================================
# SILVER — child tables exploded out of the bronze VARIANT payload
#
#   whoz_profile_aptitude       113,810 rows   PK aptitude_id
#   whoz_profile_position        15,104 rows   PK position_id (self-hierarchical)
#   whoz_position_aptitude_ref  137,050 rows   PK (position_id, aptitude_id)
#   whoz_profile_skill_rating     6,792 rows   legacy — see docs/whoz_profile_data_model.md
#
# variant_explode is a table-valued generator, so it goes in the FROM clause via
# LATERAL. That is why these use spark.sql rather than the DataFrame API — there is
# no DataFrame equivalent that unnests a VARIANT array in one step.
# =====================================================================================

from pyspark import pipelines as dp


# -------------------------------------------------------------------------------------
# APTITUDES — the skills inventory. 2,134 of 4,113 profiles have any; max 373 per profile.
# -------------------------------------------------------------------------------------
@dp.table(
    name="whoz_profile_aptitude",
    comment="One row per aptitude (skill / language / tool) declared on a profile.",
    table_properties={"quality": "silver"},
    cluster_by=["profile_id", "aptitude_type"],
)
@dp.expect_or_drop("aptitude_id_not_null", "aptitude_id IS NOT NULL")
@dp.expect("proficiency_in_range", "proficiency IS NULL OR proficiency BETWEEN 0 AND 5")
def whoz_profile_aptitude():
    return spark.sql("""
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
        FROM STREAM(whoz_profiles_bronze) AS b,
             LATERAL variant_explode(b.payload:aptitudes) AS a
    """)


# -------------------------------------------------------------------------------------
# POSITIONS — jobs and missions in one array, split by is_mission and linked by
# parent_position_id (all 1,197 parent references resolve within the file).
# -------------------------------------------------------------------------------------
@dp.table(
    name="whoz_profile_position",
    comment="One row per position (job or mission) held on a profile. Self-hierarchical.",
    table_properties={"quality": "silver"},
    cluster_by=["profile_id", "start_date"],
)
@dp.expect_or_drop("position_id_not_null", "position_id IS NOT NULL")
# Catches the "+22015-07-31" style values: end_date comes back NULL from the cast
# while the raw string is still there. Warn, don't drop.
@dp.expect("end_date_parsed", "end_date_raw IS NULL OR end_date IS NOT NULL")
@dp.expect("dates_ordered", "start_date IS NULL OR end_date IS NULL OR end_date >= start_date")
def whoz_profile_position():
    return spark.sql("""
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
        FROM STREAM(whoz_profiles_bronze) AS b,
             LATERAL variant_explode(b.payload:positions) AS p
    """)


# -------------------------------------------------------------------------------------
# POSITION -> APTITUDE bridge. No natural id on the child, so the key is the pair.
# 137,050 references against 113,810 aptitudes: the same skill is claimed on several
# positions, which is the point of the table.
# -------------------------------------------------------------------------------------
@dp.table(
    name="whoz_position_aptitude_ref",
    comment="Bridge: skills claimed on each position. Grain = (position_id, aptitude_id).",
    table_properties={"quality": "silver"},
    cluster_by=["profile_id", "position_id"],
)
@dp.expect_or_drop("keys_not_null", "position_id IS NOT NULL AND aptitude_id IS NOT NULL")
def whoz_position_aptitude_ref():
    return spark.sql("""
        SELECT
            b.profile_id,
            try_variant_get(p.value, '$.id', 'string')          AS position_id,
            try_variant_get(r.value, '$.aptitudeId', 'string')  AS aptitude_id,
            try_variant_get(r.value, '$.conceptId',  'string')  AS concept_id,
            try_variant_get(r.value, '$.name',       'string')  AS aptitude_name,
            try_variant_get(r.value, '$.type',       'string')  AS aptitude_type,
            r.pos                                               AS ordinal,
            b.ingested_at
        FROM STREAM(whoz_profiles_bronze) AS b,
             LATERAL variant_explode(b.payload:positions) AS p,
             LATERAL variant_explode(p.value:aptitudeReferences) AS r
    """)


# -------------------------------------------------------------------------------------
# SKILL RATINGS — 6,792 rows but only 83 profiles, and 6,557 of them are rating 0.
# Almost certainly superseded by aptitudes[].proficiency. Landed for completeness;
# confirm with Whoz before anyone builds a metric on it.
# -------------------------------------------------------------------------------------
@dp.table(
    name="whoz_profile_skill_rating",
    comment="Legacy skillRatings array. Sparse and mostly zero — verify before use.",
    table_properties={"quality": "silver"},
)
def whoz_profile_skill_rating():
    return spark.sql("""
        SELECT
            b.profile_id,
            b.talent_id,
            s.pos                                        AS ordinal,
            try_variant_get(s.value, '$.skill',  'string') AS skill_name,
            try_variant_get(s.value, '$.rating', 'int')    AS rating,
            b.ingested_at
        FROM STREAM(whoz_profiles_bronze) AS b,
             LATERAL variant_explode(b.payload:skillRatings) AS s
    """)


# -------------------------------------------------------------------------------------
# Not modelled on purpose — empty on every one of the 4,113 records in this extract:
#   customFields, functionalDomains, schedules, targetFunctionalDomains,
#   targetSkillRatings, positions[].customFields, headline.mobilityDestinations
#
# Rather than build empty tables, watch for them filling up. Add this expectation to
# whoz_profile and it will alert the first time Whoz starts sending any of them:
#
#   @dp.expect("unmodelled_collections_still_empty",
#              "try_cast(size(cast(payload:customFields as array<variant>)) as int) = 0 "
#              "AND try_cast(size(cast(payload:schedules as array<variant>)) as int) = 0")
#
# Same idea for qualificationIds[] and targetSkills[] — 423 and 2 values respectively,
# too thin to be worth a table today.
# -------------------------------------------------------------------------------------

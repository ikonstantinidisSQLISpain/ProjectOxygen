# =====================================================================================
# silver.whoz_profile_aptitudes — the skills inventory, one row per declared aptitude.
#
# 113,810 rows over 2,134 of 4,113 profiles; max 373 on a single profile. PK aptitude_id.
#
# See the package docstring in __init__.py for why this is SQL text taking the source
# relation as an argument, and why it lives here rather than in transformations/silver/.
# =====================================================================================


def aptitudes_sql(source: str) -> str:
    """silver.whoz_profile_aptitudes — the skills inventory, one row per declared aptitude."""
    return f"""
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
        FROM {source} AS b,
             LATERAL variant_explode(b.payload:aptitudes) AS a
    """

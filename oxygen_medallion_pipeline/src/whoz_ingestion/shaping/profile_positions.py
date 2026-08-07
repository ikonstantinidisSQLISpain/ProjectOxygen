# =====================================================================================
# silver.whoz_profile_positions — jobs and missions in one array, self-hierarchical.
#
# 15,104 rows. PK position_id. Split by is_mission and linked by parent_position_id; all
# 1,197 parent references resolve within the file.
#
# See the package docstring in __init__.py for why this is SQL text taking the source
# relation as an argument, and why it lives here rather than in transformations/silver/.
# =====================================================================================


def positions_sql(source: str) -> str:
    """silver.whoz_profile_positions — jobs and missions in one array, self-hierarchical."""
    return f"""
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
        FROM {source} AS b,
             LATERAL variant_explode(b.payload:positions) AS p
    """

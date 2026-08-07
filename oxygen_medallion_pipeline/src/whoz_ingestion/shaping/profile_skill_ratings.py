# =====================================================================================
# silver.whoz_profile_skill_ratings — the legacy skillRatings array.
#
# 6,792 rows but only 83 profiles, and 6,557 of them are rating 0. Almost certainly
# superseded by aptitudes[].proficiency. Landed for completeness; confirm with Whoz before
# anyone builds a metric on it. See docs/whoz_profile_data_model.md.
#
# The one profile collection with NO quality checks, so its dataset has no _checked view and
# no quarantine table — see transformations/silver/whoz_profile_skill_ratings.py. That
# asymmetry is deliberate rather than an omission: there is nothing about this table worth
# watching until someone decides to trust it.
#
# See the package docstring in __init__.py for why this is SQL text taking the source
# relation as an argument, and why it lives here rather than in transformations/silver/.
# =====================================================================================


def skill_ratings_sql(source: str) -> str:
    """silver.whoz_profile_skill_ratings — the legacy skillRatings array."""
    return f"""
        SELECT
            b.profile_id,
            b.talent_id,
            s.pos                                        AS ordinal,
            try_variant_get(s.value, '$.skill',  'string') AS skill_name,
            try_variant_get(s.value, '$.rating', 'int')    AS rating,
            b.ingested_at
        FROM {source} AS b,
             LATERAL variant_explode(b.payload:skillRatings) AS s
    """

# =====================================================================================
# silver.whoz_profile_completion_rules — the completionDetails map, one row per rule.
#
# The second profile collection with no quality checks, so no _checked view and no
# quarantine table. Its query lives here with the rest all the same, so it can be checked the
# same way the day it gets one.
#
# See the package docstring in __init__.py for why this is SQL text taking the source
# relation as an argument, and why it lives here rather than in transformations/silver/.
# =====================================================================================


def completion_rules_sql(source: str) -> str:
    """silver.whoz_profile_completion_rules — the completionDetails map, one row per rule.

    completionDetails is a MAP keyed by rule name, NOT a struct — and it arrives as an
    empty ARRAY [] on 723 of the 4,113 records. Exploding it to one row per (profile, rule)
    means a new scoring rule from Whoz shows up as new *rows*, not as a schema change, and
    the array-vs-object polymorphism is handled by the filter.
    """
    return f"""
        SELECT
            b.profile_id,
            b.talent_id,
            e.key                                                  AS rule_name,
            try_variant_get(e.value, '$.satisfied', 'boolean')      AS is_satisfied,
            try_variant_get(e.value, '$.weight',    'int')          AS weight,
            b.ingested_at
        FROM {source} AS b,
             LATERAL variant_explode(b.payload:completionDetails) AS e
        -- variant_explode on an object yields key/value; on the empty-array form it
        -- yields no rows at all, which is exactly what we want.
        WHERE e.key IS NOT NULL
    """

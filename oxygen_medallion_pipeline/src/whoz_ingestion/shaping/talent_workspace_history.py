# =====================================================================================
# silver.whoz_talent_workspace_history — the talent export's own history[] array.
#
# THE ONE THIS FILENAME EXISTS TO CLARIFY. This explodes out of bronze.whoz_talents, not
# bronze.whoz_profiles — it was previously in a shared children.py alongside five profile
# collections, where nothing but the query body said which bronze table it read.
#
# Not to be confused with silver.whoz_talent_versions, which is the SCD2 history of the
# talent *record* (how it changed over time, derived by AUTO CDC). This one is which
# workspaces the talent has belonged to, and comes from the source's own array. The two names
# are deliberately not both "history".
#
# See the package docstring in __init__.py for why this is SQL text taking the source
# relation as an argument, and why it lives here rather than in transformations/silver/.
# =====================================================================================


def workspace_history_sql(source: str) -> str:
    """silver.whoz_talent_workspace_history — the talent export's own history[] array.

    One row per (talent, membership period): which workspace the talent belonged to, since
    when, and under which scope.
    """
    return f"""
        SELECT
            b.talent_id,
            h.pos                                             AS ordinal,
            try_variant_get(h.value, '$.workspaceId','string') AS workspace_id,
            try_variant_get(h.value, '$.scope',      'string') AS scope,
            -- keep BOTH the parsed date and the raw string, the same way positions[] does:
            -- the parsed one is NULL for any value the cast can't handle, and you want to
            -- be able to see which.
            try_variant_get(h.value, '$.since',      'date')   AS since_date,
            try_variant_get(h.value, '$.since',      'string') AS since_raw,
            b.ingested_at
        FROM {source} AS b,
             LATERAL variant_explode(b.payload:history) AS h
    """

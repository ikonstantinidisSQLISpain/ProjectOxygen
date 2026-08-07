# =====================================================================================
# silver.whoz_user_workspace_roles — which workspaces a user belongs to, and as what.
#
# 15,258 memberships over 2,858 of 4,036 users, 1-33 workspaces each. Grain is
# (user_id, workspace_id, role).
#
# TWO EXPLODES, and the second one is a deliberate choice rather than a necessity.
#
# The first unnests `workspaceRoles`, which is a MAP keyed by workspace id when populated and
# an empty ARRAY [] when not — the same polymorphism as the profile export's
# completionDetails, and handled the same way: variant_explode on the object form yields
# key/value pairs, and on the empty-array form yields no rows at all. That is why there is no
# type test here; the `WHERE key IS NOT NULL` guard is what makes the array form disappear
# cleanly, exactly as in profile_completion_rules.py.
#
# The second unnests each membership's `roles` array, which holds EXACTLY ONE element on all
# 15,258 entries in the analysed export. Lifting roles[0] into a scalar column would produce
# the same rows today and silently drop the second role the day Whoz grants one. Exploding
# means a second role arrives as a second ROW — a number moving, not data vanishing. Same
# reasoning as completionDetails: a new value should show up as new rows, not as a schema
# change or a truncation.
#
# See the package docstring in __init__.py for why this is SQL text taking the source
# relation as an argument, and why it lives here rather than in transformations/silver/.
# =====================================================================================


def workspace_roles_sql(source: str) -> str:
    """silver.whoz_user_workspace_roles — one row per (user, workspace, role)."""
    return f"""
        SELECT
            b.user_id,
            -- The map key IS the workspace id. The entry repeats it in workspaceId, and the
            -- two agree on all 15,258 entries measured — embedded_workspace_id_agrees is the
            -- check that keeps that true, and the reason both are carried.
            w.key                                                       AS workspace_id,
            try_variant_get(w.value, '$.workspaceId', 'string')          AS embedded_workspace_id,
            -- NULL on every entry in the analysed export. Carried anyway: a column that is
            -- always null is cheap, and its first non-null value is information.
            try_variant_get(w.value, '$.workspaceExternalId', 'string')  AS workspace_external_id,
            r.pos                                                        AS role_ordinal,
            -- '$' is the whole VARIANT value: roles[] holds bare strings, not objects.
            try_variant_get(r.value, '$', 'string')                      AS role,
            b.ingested_at
        FROM {source} AS b,
             LATERAL variant_explode(b.payload:workspaceRoles) AS w,
             LATERAL variant_explode(w.value:roles) AS r
        -- variant_explode on an object yields key/value; on the empty-array form it yields no
        -- rows at all, which is exactly what we want for the 1,178 users with no memberships.
        WHERE w.key IS NOT NULL
    """

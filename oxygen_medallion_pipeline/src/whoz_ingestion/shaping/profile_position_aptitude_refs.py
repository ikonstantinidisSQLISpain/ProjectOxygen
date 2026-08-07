# =====================================================================================
# silver.whoz_position_aptitude_refs — the bridge; no natural id, the key is the pair.
#
# 137,050 references against 113,810 aptitudes: the same skill is claimed on several
# positions, which is the point of the table. Grain = (position_id, aptitude_id).
#
# Doubly nested — positions[] then each position's aptitudeReferences[] — so it is the one
# query here with two LATERAL variant_explode clauses.
#
# See the package docstring in __init__.py for why this is SQL text taking the source
# relation as an argument, and why it lives here rather than in transformations/silver/.
# =====================================================================================


def aptitude_refs_sql(source: str) -> str:
    """silver.whoz_position_aptitude_refs — the bridge; no natural id, the key is the pair."""
    return f"""
        SELECT
            b.profile_id,
            try_variant_get(p.value, '$.id', 'string')          AS position_id,
            try_variant_get(r.value, '$.aptitudeId', 'string')  AS aptitude_id,
            try_variant_get(r.value, '$.conceptId',  'string')  AS concept_id,
            try_variant_get(r.value, '$.name',       'string')  AS aptitude_name,
            try_variant_get(r.value, '$.type',       'string')  AS aptitude_type,
            r.pos                                               AS ordinal,
            b.ingested_at
        FROM {source} AS b,
             LATERAL variant_explode(b.payload:positions) AS p,
             LATERAL variant_explode(p.value:aptitudeReferences) AS r
    """

# =====================================================================================
# Pure transformation logic for the whoz_user silver table.
#
# Same contract as shaping/profile.py and shaping/talent.py: NO dependency on
# `pyspark.pipelines`, plain DataFrame in / DataFrame out, so it can be unit tested against a
# local SparkSession. silver/whoz_user.py imports shape_user() and wraps it with @dp.table.
#
# WHAT A USER IS, AND IS NOT
#
# This is the **login account**, not the person. silver.whoz_talents is a person in a
# workspace. The two are not 1:1 and neither contains the other:
#
#   * 2,275 of 4,116 talents carry a userId; the rest have none
#   * 1,760 of 4,036 users have no talent at all — service and administrative accounts
#
# The foreign key lives on the talent side (talent.userId -> user.id) and stays there. This
# table deliberately carries no talent_id: 44% of users would have a null one, and the column
# would have to be rebuilt every time a talent appeared. Join from talent, not from here.
#
# WHAT THIS DOES NOT MODEL, and why — see docs/whoz_user_data_model.md §5 for the full list:
#
#   * formerUsernames VALUES. Count only. They are previous login addresses (personal data)
#     and some encode GDPR erasure requests. The raw array stays in bronze's VARIANT payload
#     forever, so a child table is possible later without a re-ingest.
#   * agenticStudioRoles. Empty on every record; a count column plus a check that watches it.
#   * workspaceRoles. Not flattened here — it is silver.whoz_user_workspace_roles, one row
#     per (user, workspace, role). See shaping/user_workspace_roles.py.
# =====================================================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from whoz_ingestion.contract import SCD2_COLUMNS, ddl, load_columns
from whoz_ingestion.shaping import collection_size_sql

# The declared schema of silver.whoz_users / silver.whoz_user_versions. The columns are data
# in ../schemas/whoz_user.yml, contract.ddl() renders them, and they must match shape_user()'s
# SELECT column for column and in order — tests/layer2_contract/test_user_contract.py asserts
# exactly that.
USER_COLUMN_DEFS = load_columns("whoz_user")
USER_COLUMNS = ddl(USER_COLUMN_DEFS)
USER_HISTORY_COLUMNS = ddl(USER_COLUMN_DEFS + SCD2_COLUMNS)


def vg(path, target_type):
    """try_variant_get on the payload column — NULL on missing path or bad cast."""
    return F.try_variant_get("payload", path, target_type)


def count_of(path: str) -> "F.Column":
    """Number of entries in a collection that may be a MAP *or* an empty ARRAY.

    DO NOT REPLACE THIS WITH a plain `cast(payload:<path> as array<variant>)`. That raises
    INVALID_VARIANT_CAST when the value is an object rather than returning NULL, and it fails
    on every record whose federationRoles is populated. `try_variant_get` is the lenient
    form — NULL when the value is not the requested type — so asking for both shapes and
    coalescing gives the three answers we want:

        populated (object form)  -> size of the map      (1, or 1-33 for workspaces)
        empty     (array form)   -> 0
        key absent entirely      -> NULL

    0 and NULL are deliberately different: "this user belongs to no workspaces" is not the
    same fact as "this export did not say", and only the sparse records produce the latter.

    Both branches go through collection_size_sql because `size(NULL)` is -1 on Databricks and
    NULL locally. Written the obvious way, the map branch returned -1 for every array-form
    record, coalesce accepted it, and federation_count_at_most_one fired on 1,173 real rows.
    See collection_size_sql's docstring — that is a deploy-only failure, and this is the
    second-order reason it matters: coalesce cannot fall through a -1.
    """
    map_form = f"try_variant_get(payload, '$.{path}', 'map<string,variant>')"
    array_form = f"try_variant_get(payload, '$.{path}', 'array<variant>')"
    return F.expr(f"coalesce({collection_size_sql(map_form)}, {collection_size_sql(array_form)})")


def array_count_of(path: str) -> "F.Column":
    """size() of a collection that is always a plain array. NULL when the key is absent."""
    return F.expr(collection_size_sql(f"try_variant_get(payload, '$.{path}', 'array<string>')"))


def federation(field: str) -> "F.Column":
    """A field off the single federationRoles entry, or NULL when there is no map form.

    Safe to index [0] because the map is strictly one entry on every populated record in the
    analysed export — 2,863 of 2,863. `federation_count_at_most_one` is the DQX check that
    makes that assumption falsifiable; the day it fires, this flattening is wrong and the
    federation membership needs its own child table the way workspace membership has one.
    """
    return F.expr(
        f"try_cast(map_values(try_variant_get(payload, '$.federationRoles', "
        f"'map<string,variant>'))[0]:{field} as string)"
    )


def shape_user(bronze: DataFrame) -> DataFrame:
    """Bronze columns -> whoz_user columns."""
    return bronze.select(
        # ---- identity (already lifted out of the payload by bronze) ----
        F.col("user_id"),
        F.col("idp_id"),
        # ---- account attributes ----
        vg("$.username", "string").alias("username"),
        vg("$.enabled", "boolean").alias("is_enabled"),
        vg("$.removed", "boolean").alias("is_removed"),
        vg("$.language", "string").alias("language"),
        vg("$.theme", "string").alias("theme"),
        # ---- federation, flattened: strictly 1:1 today, see federation() ----
        # The map KEY rather than the entry's own federationId: they agree on every record
        # (measured, 0 mismatches), and the key is what a map is addressed by.
        F.expr(
            "try_cast(map_keys(try_variant_get(payload, '$.federationRoles', 'map<string,variant>'))[0] as string)"
        ).alias("federation_id"),
        federation("roles[0]").alias("federation_role"),
        count_of("federationRoles").alias("federation_count"),
        # ---- timestamps ----
        # Second- AND millisecond-precision values occur in the same file; casting to
        # timestamp handles both, a fixed format string would handle one and NULL the other.
        vg("$.lastConnectionDate", "timestamp").alias("last_connection_at"),
        # ---- audit fields from the source system ----
        vg("$.createdDate", "timestamp").alias("source_created_at"),
        vg("$.createdBy", "string").alias("source_created_by"),
        vg("$.lastModifiedDate", "timestamp").alias("source_last_modified_at"),
        # NULLIF, and it is the one normalisation this module performs. The source sends the
        # four-character STRING "null" on 983 of 4,036 records — not JSON null, which appears
        # exactly once. Left alone it is a sentinel that joins to nothing and reads as a real
        # id in every GROUP BY. Normalised here rather than downstream so the fix happens once
        # rather than being rediscovered by each consumer. createdBy does not have this
        # problem, so it is passed through untouched.
        F.nullif(vg("$.lastModifiedBy", "string"), F.lit("null")).alias("source_last_modified_by"),
        # ---- collection sizes: cheap, and they make quality drift obvious ----
        count_of("workspaceRoles").alias("workspace_role_count"),
        array_count_of("formerUsernames").alias("former_username_count"),
        array_count_of("agenticStudioRoles").alias("agentic_studio_role_count"),
        # No payload column in the OUTPUT — same reason as shape_profile/shape_talent: AUTO
        # CDC compares whole rows with `<=>` to detect real changes and VARIANT does not
        # support that comparison (INVALID_ORDERING_TYPE). Bronze keeps the raw payload
        # forever; join back on user_id.
        # ---- lineage ----
        F.col("source_file"),
        F.col("ingested_at"),
    )

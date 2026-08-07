# =====================================================================================
# BRONZE — bronze.whoz_users
#
# One row per user account object from the Whoz user export. The entire JSON object is
# stored untouched in a single VARIANT column ("payload"). Nothing is inferred, nothing is
# cast, nothing can drift.
#
# Structurally identical to bronze/whoz_talents.py and bronze/whoz_profiles.py — same
# pretty-printed JSON *array* root, so the same multiLine + singleVariantColumn +
# variant_explode treatment applies. Read whoz_profiles.py's header for why: multiLine loads
# the whole file as one entity and singleVariantColumn puts all of it into one VARIANT value
# in one row, so the array has to be exploded explicitly to get one row per user.
#
# A user is the LOGIN ACCOUNT, not the person — silver.whoz_talents is a person in a
# workspace. The two are not 1:1 in either direction. See whoz_ingestion/shaping/user.py's
# header and docs/whoz_user_data_model.md §1.
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Set from the pipeline's 'configuration' block in
# resources/whoz_ingestion_etl.pipeline.yml — that yml is the only source of truth for
# these, so no fallback value here: a deploy missing this config should fail loudly
# (NoSuchElementException naming the key) instead of silently landing on someone else's
# volume or catalog.
SOURCE_PATH = spark.conf.get("whoz.users.source_path")
SCHEMA_PATH = spark.conf.get("whoz.users.schema_path")
# The 'source' volume is a shared landing zone, not Whoz-specific, so filter to just our
# files. Matches both the bare export name and a "YYYY-MM-DD_" generation-date prefix, the
# same convention the profile and talent exports use. Confirmed against the real file, which
# lands bare: whoz__user_report_anonymized.json.
FILE_NAME_GLOB = "*whoz__user_report_anonymized.json"

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_users"


@dp.table(
    name=BRONZE_TABLE,
    comment=(
        "Raw Whoz user export, one row per account object. Full JSON kept as-is in the "
        "VARIANT column 'payload' — no schema inference, immune to source drift."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
    cluster_by=["ingest_date", "user_id"],
)
def whoz_users():
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("singleVariantColumn", "payload")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("pathGlobFilter", FILE_NAME_GLOB)
        .load(SOURCE_PATH)
        # _metadata is a hidden struct that doesn't survive createOrReplaceTempView below.
        # Pull the fields we need into plain columns first, while it's still a real
        # DataFrame.
        .select(
            "payload",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_name").alias("_source_file_name"),
            F.col("_metadata.file_size").alias("_source_file_size"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        )
    )

    # The temp view is NOT redundant, however much it looks it — see the long note in
    # bronze/whoz_profiles.py. Binding a DataFrame into spark.sql(..., raw=raw) fails
    # inside a real pipeline with "_dlt_sql_fn() got an unexpected keyword argument", and
    # only there: local pytest and `databricks bundle validate` both pass happily.
    raw.createOrReplaceTempView("_whoz_users_raw")

    return spark.sql("""
        SELECT
            -- try_variant_get returns NULL rather than raising if the path is missing,
            -- so a malformed record still lands instead of killing the batch. These two
            -- paths must match BRONZE_KEYS['whoz_users'] in tests/conftest.py, or the tests
            -- build their input by a different route than production reads it.
            try_variant_get(e.value, '$.id', 'string')           AS user_id,
            try_variant_get(e.value, '$.idpId', 'string')        AS idp_id,
            -- ---- THE PAYLOAD: one user object, not the whole array ----
            e.value                                             AS payload,
            -- ---- INGESTION METADATA ----
            b._source_file                                      AS source_file,
            b._source_file_name                                 AS source_file_name,
            b._source_file_size                                 AS source_file_size,
            b._source_file_modified_at                          AS source_file_modified_at,
            current_timestamp()                                 AS ingested_at,
            current_date()                                      AS ingest_date,
            'whoz'                                              AS source_system,
            'user_report'                                       AS source_entity,
            -- Cheap drift sensors. The first is the same top-level key list bronze keeps for
            -- the other entities — it is doing real work here, since this export has three
            -- distinct key sets (idpId and lastConnectionDate are absent on 42% of records).
            --
            -- The other two are specific to this export's real structural risk: workspaceRoles
            -- and federationRoles are an OBJECT when populated and an empty ARRAY when not.
            -- Group by either to see the split, and to notice the day a third form appears.
            array_join(array_sort(map_keys(cast(e.value as map<string, variant>))), ',')
                                                                AS payload_top_level_keys,
            regexp_extract(schema_of_variant(e.value:workspaceRoles), '^([A-Za-z]+)', 1)
                                                                AS workspace_roles_container_type,
            regexp_extract(schema_of_variant(e.value:federationRoles), '^([A-Za-z]+)', 1)
                                                                AS federation_roles_container_type
        FROM _whoz_users_raw AS b,
             LATERAL variant_explode(b.payload) AS e
    """)


# -------------------------------------------------------------------------------------
# Drift monitor, same idea as whoz_talents_payload_shapes. A materialized view, so it never
# blocks ingestion — it just makes a new or vanished field visible the day it happens, and
# (via the two container-type columns) the day either polymorphic map takes a third form.
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.whoz_users_payload_shapes",
    comment="Distinct top-level key sets and role-map container types seen in the bronze user payload.",
)
def whoz_users_payload_shapes():
    return (
        spark.read.table(BRONZE_TABLE)
        .groupBy("payload_top_level_keys", "workspace_roles_container_type", "federation_roles_container_type")
        .agg(
            F.count("*").alias("record_count"),
            F.min("ingested_at").alias("first_seen_at"),
            F.max("ingested_at").alias("last_seen_at"),
        )
        .orderBy(F.desc("record_count"))
    )

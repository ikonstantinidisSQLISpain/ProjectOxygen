# =====================================================================================
# BRONZE — bronze.whoz_profiles
#
# One row per profile object from the Whoz export. The entire JSON object is stored
# untouched in a single VARIANT column ("payload"). Nothing is inferred, nothing is
# cast, nothing can drift: if Whoz adds, removes or retypes a field, this table keeps
# ingesting and the change surfaces downstream instead of failing the load.
#
# Source is a pretty-printed JSON *array* (not JSONL). Confirmed the hard way against
# a live run: multiLine + singleVariantColumn do NOT give one row per array element —
# multiLine loads the whole file as a single entity, and singleVariantColumn puts all
# of it into one VARIANT value in one row. So `raw` below is one row holding the whole
# 4,113-element array as a single VARIANT, and we explicitly explode that array with
# variant_explode — the same table-valued-function pattern already used in
# silver_whoz_profile_children.py for positions[]/aptitudes[], just applied one level
# higher, at the array root instead of a nested field.
#
# See docs/whoz_profile_data_model.md for the field-level analysis these choices are
# based on (that doc predates this fix and still describes the intended per-profile
# shape correctly — just not the mechanism that gets you there).
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Set from the pipeline's 'configuration' block in
# resources/whoz_ingestion_etl.pipeline.yml — that yml is the only source of truth
# for these, so no fallback value here: a deploy missing this config should fail
# loudly (NoSuchElementException naming the key) instead of silently landing on
# someone else's volume or catalog.
SOURCE_PATH = spark.conf.get("whoz.profiles.source_path")
SCHEMA_PATH = spark.conf.get("whoz.profiles.schema_path")
# The 'source' volume is a shared landing zone, not Whoz-specific, so filter to just
# our files. Matches both the bare export name and a "YYYY-MM-DD_" generation-date
# prefix, e.g. "2026-07-20_whoz__profile_report_anonymized.json" — Auto Loader picks
# up each new dated file as it lands, no path change needed when the date rolls.
FILE_NAME_GLOB = "*whoz__profile_report_anonymized.json"

# Fully-qualified table names, built from pipeline config rather than hardcoded, so
# bronze/silver can land in the same schema in dev (personal sandbox, no access-control
# need) but separate schemas in prod (bronze holds raw, not-fully-anonymized payloads —
# see docs/whoz_profile_data_model.md). See resources/whoz_ingestion_etl.pipeline.yml
# and databricks.yml's bronze_schema/silver_schema variables.
CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles"


@dp.table(
    name=BRONZE_TABLE,
    comment=(
        "Raw Whoz profile export, one row per profile object. Full JSON kept as-is in "
        "the VARIANT column 'payload' — no schema inference, immune to source drift."
    ),
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
    cluster_by=["ingest_date", "profile_id"],
)
def whoz_profiles():
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        # Loads the whole file as one entity — required for a JSON array root, but see
        # the module docstring: this means one row per FILE here, not per profile yet.
        .option("multiLine", "true")
        # Puts that one row's entire content into a single VARIANT column, "payload".
        # No inferred schema, so no schemaHints, no rescuedDataColumn, no evolution
        # restarts, no "cannot cast" failures on records like the +22015-07-31 endDate.
        .option("singleVariantColumn", "payload")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("pathGlobFilter", FILE_NAME_GLOB)
        .load(SOURCE_PATH)
        # _metadata is a hidden struct that doesn't survive createOrReplaceTempView
        # below — confirmed the hard way. Pull the fields we need into plain columns
        # first, while it's still a real DataFrame.
        .select(
            "payload",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_name").alias("_source_file_name"),
            F.col("_metadata.file_size").alias("_source_file_size"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        )
    )

    # The temp view is NOT redundant, however much it looks it. Spark 3.4+ lets you bind
    # a DataFrame straight into a query — spark.sql("... FROM {raw}", raw=raw) — and that
    # is the obvious simplification here, but Lakeflow replaces spark.sql with its own
    # wrapper whose signature takes no such kwargs. It fails at flow-analysis time with
    # "_dlt_sql_fn() got an unexpected keyword argument 'raw'", and only inside a real
    # pipeline: local pytest and `databricks bundle validate` both pass happily. Tried
    # it, got exactly that. Leave the view.
    raw.createOrReplaceTempView("_whoz_profiles_raw")

    # variant_explode is a table-valued generator, so it goes in the FROM clause via
    # LATERAL — there is no DataFrame equivalent that unnests a VARIANT array in one
    # step (same reason silver_whoz_profile_children.py uses spark.sql for
    # positions[]/aptitudes[]). b.payload here is the whole array; e.value is one
    # profile object per row, which is what makes this one row per profile at last.
    return spark.sql("""
        SELECT
            -- try_variant_get returns NULL rather than raising if the path is
            -- missing, so a malformed record still lands instead of killing the batch.
            try_variant_get(e.value, '$.id', 'string')          AS profile_id,
            try_variant_get(e.value, '$.talentId', 'string')    AS talent_id,
            try_variant_get(e.value, '$.federationId', 'string') AS federation_id,
            -- ---- THE PAYLOAD: one profile object, not the whole array ----
            e.value                                             AS payload,
            -- ---- INGESTION METADATA ----
            b._source_file                                      AS source_file,
            b._source_file_name                                 AS source_file_name,
            b._source_file_size                                 AS source_file_size,
            b._source_file_modified_at                          AS source_file_modified_at,
            current_timestamp()                                 AS ingested_at,
            current_date()                                      AS ingest_date,
            'whoz'                                              AS source_system,
            'profile_report'                                    AS source_entity,
            -- Cheap drift sensor: the sorted top-level key list of this profile.
            -- Group by this column to see every distinct payload shape in the table.
            array_join(array_sort(map_keys(cast(e.value as map<string, variant>))), ',')
                                                                 AS payload_top_level_keys
        FROM _whoz_profiles_raw AS b,
             LATERAL variant_explode(b.payload) AS e
    """)


# -------------------------------------------------------------------------------------
# Optional but recommended: a drift monitor built on the sensor column above.
# It is a materialized view, so it never blocks ingestion — it just makes a new or
# vanished field visible the day it happens.
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles_payload_shapes",
    comment="Distinct top-level key sets seen in the bronze payload, with first/last seen.",
)
def whoz_profiles_payload_shapes():
    return (
        spark.read.table(BRONZE_TABLE)
        .groupBy("payload_top_level_keys")
        .agg(
            F.count("*").alias("record_count"),
            F.min("ingested_at").alias("first_seen_at"),
            F.max("ingested_at").alias("last_seen_at"),
        )
        .orderBy(F.desc("record_count"))
    )


# -------------------------------------------------------------------------------------
# Notes on the source file that drove these choices:
#
#  * completionDetails is a JSON OBJECT on 3,390 records and an empty ARRAY [] on 723.
#    Any struct/map inference breaks on that. VARIANT holds both.
#  * positions[] has 26 distinct key sets, aptitudes[] has 10, the root has 6.
#  * completionRate and aptitudes[].cumulativeExperience are int on some records and
#    float on others.
#  * Timestamps appear at second, millisecond AND nanosecond precision in the same field.
#  * One positions[].endDate is "+22015-07-31".
#
# All of these are handled by keeping the payload as VARIANT and casting lazily in
# silver with try_variant_get.
# -------------------------------------------------------------------------------------

# =====================================================================================
# BRONZE — whoz_profiles_bronze
#
# One row per profile object from the Whoz export. The entire JSON object is stored
# untouched in a single VARIANT column ("payload"). Nothing is inferred, nothing is
# cast, nothing can drift: if Whoz adds, removes or retypes a field, this table keeps
# ingesting and the change surfaces downstream instead of failing the load.
#
# Source is a pretty-printed JSON *array* (not JSONL), so multiLine is mandatory.
# Spark yields one row per array element.
#
# See docs/whoz_profile_data_model.md for the analysis these choices are based on.
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Set from the pipeline's 'configuration' block in
# resources/whoz_ingestion_etl.pipeline.yml. The defaults below keep a bare "Run file"
# in the workspace working when no configuration is supplied.
SOURCE_PATH = spark.conf.get("whoz.profiles.source_path", "/Volumes/main/landing/whoz/profiles/")
SCHEMA_PATH = spark.conf.get("whoz.profiles.schema_path", "/Volumes/main/landing/whoz/_schema/profiles")


@dp.table(
    name="whoz_profiles_bronze",
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
def whoz_profiles_bronze():
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        # The root of the file is a JSON array spanning many lines.
        .option("multiLine", "true")
        # THE KEY OPTION: the whole element becomes one VARIANT column called "payload".
        # No inferred schema, so no schemaHints, no rescuedDataColumn, no evolution
        # restarts, no "cannot cast" failures on records like the +22015-07-31 endDate.
        .option("singleVariantColumn", "payload")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .load(SOURCE_PATH)
    )

    return raw.select(
        # ---- business keys lifted out for clustering / dedup only ----
        # try_variant_get returns NULL rather than raising if the path is missing,
        # so a malformed record still lands instead of killing the batch.
        F.try_variant_get("payload", "$.id", "string").alias("profile_id"),
        F.try_variant_get("payload", "$.talentId", "string").alias("talent_id"),
        F.try_variant_get("payload", "$.federationId", "string").alias("federation_id"),
        # ---- THE PAYLOAD ----
        F.col("payload"),
        # ---- INGESTION METADATA ----
        F.col("_metadata.file_path").alias("source_file"),
        F.col("_metadata.file_name").alias("source_file_name"),
        F.col("_metadata.file_size").alias("source_file_size"),
        F.col("_metadata.file_modification_time").alias("source_file_modified_at"),
        F.current_timestamp().alias("ingested_at"),
        F.current_date().alias("ingest_date"),
        F.lit("whoz").alias("source_system"),
        F.lit("profile_report").alias("source_entity"),
        # Cheap drift sensor: the sorted top-level key list of this record.
        # Group by this column to see every distinct payload shape in the table.
        F.expr("array_join(array_sort(map_keys(cast(payload as map<string, variant>))), ',')")
        .alias("payload_top_level_keys"),
    )


# -------------------------------------------------------------------------------------
# Optional but recommended: a drift monitor built on the sensor column above.
# It is a materialized view, so it never blocks ingestion — it just makes a new or
# vanished field visible the day it happens.
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="whoz_profiles_payload_shapes",
    comment="Distinct top-level key sets seen in the bronze payload, with first/last seen.",
)
def whoz_profiles_payload_shapes():
    return (
        spark.read.table("whoz_profiles_bronze")
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

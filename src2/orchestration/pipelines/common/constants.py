CATALOG = lambda spark: spark.conf.get("read.catalog")
RAW_SCHEMA = lambda spark: spark.conf.get("raw.schema")
BRONZE_SCHEMA = lambda spark: spark.conf.get("bronze.schema")
SILVER_SCHEMA = lambda spark: spark.conf.get("silver.schema")
GOLD_SCHEMA = lambda spark: spark.conf.get("gold.schema")

METADATA_COLUMNS = [
    "_source_file",
    "_source_file_name",
    "_source_file_size",
    "_source_file_modified_at"
]
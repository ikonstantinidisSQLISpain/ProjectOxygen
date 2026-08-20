

CATALOG = lambda spark: spark.conf.get("read.catalog")
READ_SCHEMA = lambda spark: spark.conf.get("bronze.schema")
TARGET_SCHEMA = lambda spark: spark.conf.get("silver.schema")
MAPPING_SCHEMA = lambda spark: spark.conf.get("mapping.schema")

METADATA_COLUMNS = [
    " _source_file",
    "_source_file_name",
    "_source_file_size",
    "_source_file_modified_at"
]
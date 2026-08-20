
CATALOG = lambda spark: spark.conf.get("read.catalog")
READ_SCHEMA = lambda spark: spark.conf.get("raw.schema")
TARGET_SCHEMA = lambda spark: spark.conf.get("bronze.schema")
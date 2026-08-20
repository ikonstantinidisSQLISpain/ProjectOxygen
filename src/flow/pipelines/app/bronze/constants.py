CATALOG = lambda spark: spark.conf.get("read.catalog")
TARGET_SCHEMA = lambda spark: spark.conf.get("bronze.schema")
READ_SCHEMA = lambda spark: spark.conf.get("raw.schema")
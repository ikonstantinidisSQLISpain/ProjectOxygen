CATALOG = lambda spark: spark.conf.get("read.catalog")
TARGET_SCHEMA = lambda spark: spark.conf.get("target.schema")
READ_SCHEMA = lambda spark: spark.conf.get("read.schema")
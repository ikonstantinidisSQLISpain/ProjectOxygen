# =====================================================================================
# SILVER — silver.whoz_profile_completion_rules
#
# One row per (profile, completion rule), exploded out of the completionDetails map.
#
# Previously lived at the bottom of silver/whoz_profile.py, which made it the one child
# table not filed under its own name. It is here now for the same reason every other table
# in this folder is: the filename is the table name, so "where is this table defined" needs
# no search.
#
# ONE OBJECT, NOT THREE: no entry in whoz_ingestion/checks/, so nothing to apply, no valid
# view and no quarantine table. The query is in
# whoz_ingestion/shaping/profile_completion_rules.py, where a test can reach it and where it
# can be checked the same way as the rest the day it gets a check list.
# =====================================================================================

from pyspark import pipelines as dp

from whoz_ingestion.shaping.profile_completion_rules import completion_rules_sql

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles"


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_completion_rules",
    comment="One row per (profile, completion rule) — unpacked from the completionDetails map.",
    table_properties={"quality": "silver"},
)
def whoz_profile_completion_rules():
    return spark.sql(completion_rules_sql(f"STREAM({BRONZE_TABLE})"))

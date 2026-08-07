# =====================================================================================
# SILVER — silver.whoz_profile_skill_ratings
#
# 6,792 rows but only 83 profiles, and 6,557 of them are rating 0. Almost certainly
# superseded by aptitudes[].proficiency. Landed for completeness; confirm with Whoz before
# anyone builds a metric on it.
#
# ONE OBJECT, NOT THREE, and that asymmetry is the point rather than an omission: this
# dataset has no entry in whoz_ingestion/checks/, so there is nothing to apply, no valid
# view to take, and no quarantine table to write. test_rule_hygiene.py checks that
# correspondence both ways, so a checks file appearing for this dataset without the wiring
# below being extended fails locally.
#
# The query is in whoz_ingestion/shaping/profile_skill_ratings.py; only the streaming read of
# bronze is decided here.
# =====================================================================================

from pyspark import pipelines as dp

from whoz_ingestion.shaping.profile_skill_ratings import skill_ratings_sql

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles"

# No DQEngine in this module: nothing here applies checks. Importing whoz_ingestion.dq would
# not construct one either (it is lazy), but not importing it at all says so more plainly.


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_skill_ratings",
    comment="Legacy skillRatings array. Sparse and mostly zero — verify before use.",
    table_properties={"quality": "silver"},
)
def whoz_profile_skill_ratings():
    return spark.sql(skill_ratings_sql(f"STREAM({BRONZE_TABLE})"))

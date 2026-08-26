import pyspark.pipelines as dp
import pyspark.sql.functions as F

from silver_constants import CATALOG, READ_SCHEMA, TARGET_SCHEMA
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)


"""
@dp.materialized_view(
    name=f"{CATALOG}.{TARGET_SCHEMA}.qualifications",
    comment=(
        f"Qualifications"
    ),
    table_properties={
        "quality":"silver"
    }
)
def get_unique_qualifications():

    certs = spark.read.table(f"{CATALOG}.{READ_SCHEMA}.certifications")
    profiles = spark.read.table(f"{CATALOG}.{READ_SCHEMA}.profiles")

    

    return qualis

"""
import pyspark.pipelines as dp
import pyspark.sql.functions as F
from bronze_constants import CATALOG, TARGET_SCHEMA, READ_SCHEMA
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)

@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.societies",
    comment=(
        "Raw societies data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_societies_table():

    
    # We load the json
    raw = spark.read.option("multiLine", True)\
                    .json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/analytic_society_anonymized.json")
    # We let it infer the schema by itself and we select what we want

    df = raw.select(
        F.col("id").astype("long"),
        F.col("active").astype("boolean"),
        F.col("name"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at")
    )
    return df
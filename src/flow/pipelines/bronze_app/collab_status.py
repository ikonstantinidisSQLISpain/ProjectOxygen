import pyspark.pipelines as dp
import pyspark.sql.functions as F
from constants import CATALOG, TARGET_SCHEMA, READ_SCHEMA
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)


@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.collab_status",
    comment=(
        "Raw collaboration status data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_collab_status_table():

    
    # We load the json
    raw = spark.read.option("multiLine", True)\
                    .json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/perso_collab_status_report_anonymized.json")
    # We let it infer the schema by itself and we select what we want

    df = raw.select(
        F.col("uid").alias("worker_id"),
        F.col("department_id").astype("long"),
        F.col("service_line_id").astype("long"),
        F.col("year").astype("int"),
        F.col("month").astype("int"),
        F.col("status"),
        F.col("standard_cost_category"),
        ###############################################3
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at")
    )
    return df
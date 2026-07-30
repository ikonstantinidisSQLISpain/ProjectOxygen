import pyspark.pipelines as dp
import pyspark.sql.functions as F


CATALOG = spark.conf.get("catalog")
SCHEMA = spark.conf.get("schema")


dp.table(
    name="bu"
    comment=(
        "Raw bu data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_bu_table():

    # First we create the schema if they dont exist
    spark.sql("CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    # We load the json
    raw = spark.read.json(f"/Volumes/{CATALOG}/landing/source/analytic_bu_anonymized.json")
    # We let it infer the schema by itself and we select what we want

    df = raw.select(
        F.col("id").astype("long"),
        F.col("active").astype("bool"),
        F.col("type"),
        F.col("symbole"),
        F.col("name"),
        F.col("calendar"),
        F.col("society.id").alias("society_id"),
        F.col("entity.id").alias("entity_id"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at")
    )
    return df

import pyspark.pipelines as dp
import pyspark.sql.functions as F


#CATALOG = spark.conf.get("catalog")
#SCHEMA = spark.conf.get("schema")
CATALOG = "churndefender_poc"
SCHEMA = "bronze_app"

@dp.table(
    name=f"{CATALOG}.{SCHEMA}.worker",
    comment=(
        "Raw worker data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_bu_table():

    
    # We load the json
    raw = spark.read.option("multiLine", True).json("/Volumes/oxygen_dev/landing/source/perso_workers_anonymized.json")
    # We let it infer the schema by itself and we select what we want

    df = raw.select(
        F.col("id"), # Must be string cause IDs are strings
        F.col("active").astype("boolean"),
        F.col("start_date"),
        F.col("seniority_date"),
        F.col("job_title"),
        F.col("fulltime_or_parttime"),
        F.col("productivity_coefficient"),
        F.col("gcm"),
        F.col("std_cost"),
        F.col("tariff"),
        F.col("skill"),
        F.col("position"),
        F.col("employee_category"),
        F.col("bucu.id").astype("long").alias("bu_id"),
        F.col("site.id").astype("long").alias("site_id"),
        F.col("tl_rh.id").alias("tl_rh_id"),
        F.col("direct_manager.id").alias("direct_manager_id"),
        ###################################################
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at")
    )
    return df
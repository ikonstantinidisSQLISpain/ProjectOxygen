import pyspark.pipelines as dp
import pyspark.sql.functions as F


#CATALOG = spark.conf.get("catalog")
#SCHEMA = spark.conf.get("schema")
CATALOG = "churndefender_poc"
SCHEMA = "bronze_app"

@dp.table(
    name=f"{CATALOG}.{SCHEMA}.service_lines",
    comment=(
        "Raw service lines data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_service_lines_table():

    
    # We load the json
    raw = spark.read.option("multiLine", True)\
                    .json("/Volumes/oxygen_dev/landing/source/analytic_service_line_anonymized.json")
    # We let it infer the schema by itself and we select what we want

    df = raw.select(
        F.col("id").astype("long"),
        F.col("active").astype("boolean"),
        F.col("name"),
        F.col("code"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at")
    )
    return df



@dp.table(
    name=f"{CATALOG}.{SCHEMA}.service_lines_zones",
    comment=(
        "Raw service lines and zones relations data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_service_lines_zones_table():

    
    # We load the json
    raw = spark.read.option("multiLine", True)\
                    .json("/Volumes/oxygen_dev/landing/source/analytic_service_line_anonymized.json")
    # We let it infer the schema by itself and we select what we want

    df = raw.select(
        F.col("id").alias("service_line_id"),
        F.explode(F.col("associated_zone")).alias("zone_id")
    ).dropna("any")
    return df



@dp.table(
    name=f"{CATALOG}.{SCHEMA}.service_lines_departments",
    comment=(
        "Raw service lines and departments relations data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_service_lines_departments_table():

    
    # We load the json
    raw = spark.read.option("multiLine", True)\
                    .json("/Volumes/oxygen_dev/landing/source/analytic_service_line_anonymized.json")
    # We let it infer the schema by itself and we select what we want

    df = raw.select(
        F.col("id").alias("service_line_id"),
        F.explode(F.col("associated_practice")).alias("department_id")
    ).dropna("any")
    return df
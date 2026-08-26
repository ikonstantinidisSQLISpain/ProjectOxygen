import pyspark.pipelines as dp
import pyspark.sql.functions as F
from bronze_constants import CATALOG, TARGET_SCHEMA, READ_SCHEMA
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)


@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.departments",
    comment=(
        "Raw departments data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_departments_table():

    
    # We load the json
    raw = spark.read.option("multiLine", True)\
                    .json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/analytic_department_anonymized.json")
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
    name=f"{CATALOG}.{TARGET_SCHEMA}.zones",
    comment=(
        "Raw zones data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_zones_table():

    
    # We load the json
    department = spark.read.option("multiLine", True)\
                    .json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/analytic_department_anonymized.json")
    service_line = spark.read.option("multiLine", True)\
                    .json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/analytic_service_line_anonymized.json")
    # We let it infer the schema by itself and we select what we want
    zones_d = department.select(F.explode(F.col("associated_zone")).alias("zone_id")).distinct()
    zones_sl = service_line.select(F.explode(F.col("associated_zone")).alias("zone_id")).distinct()

    df = zones_d.union(zones_sl).distinct().select("zone_id").alias("id")
    return df



@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.department_service_lines_zones",
    comment=(
        "Raw departments and zones relations data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_departments_zones_tables():
    department = spark.read.option("multiLine", True)\
                    .json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/analytic_department_anonymized.json")

    df = department.select(
        F.col("id"),
        F.explode(F.col("associated_service_line")).alias("service_line_id"),
        F.explode(F.col("associated_zone")).alias("zone_id")
    ).dropna(how="any")
    return df







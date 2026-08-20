import pyspark.pipelines as dp
import pyspark.sql.functions as F
from src.flow.pipelines.app.bronze.constants import CATALOG, TARGET_SCHEMA, READ_SCHEMA
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)

@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.timesheet",
    comment=(
        "Raw timesheet data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_timesheet_table():

    
    # We load the json
    raw = spark.read.option("multiLine", True)\
                    .json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/app_timesheet_report_anonymized.json")
    # We let it infer the schema by itself and we select what we want

    df = raw.select(
        F.col("uid").alias("worker_id"),
        F.col("year").astype("int"),
        F.col("month").astype("int"),
        F.col("imputation").astype("int"),
        F.col("type_id").alias("activity_type_id"),
        ###############################################3
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at")
    )
    return df



@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.activity",
    comment=(
        "Raw activity data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_activity_table():

    
    # We load the json
    raw = spark.read.option("multiLine", True)\
                    .json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/app_timesheet_report_anonymized.json")
    # We let it infer the schema by itself and we select what we want

    df = raw.select(
        F.col("type_id"),
        F.col("type_lib"),
        F.col("activity"),
        F.col("is_project").astype("boolean"),
        ###############################################3
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at")
    ).dropDuplicates()
    return df



@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.activity_service_line",
    comment=(
        "Raw activity data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_activity_service_line_table():
    raw = spark.read.option("multiLine", True)\
                        .json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/app_timesheet_report_anonymized.json")

    # They are already unpacked so no need to analyze anything, just
    # select the type_id and service_line
    df = raw.select(
        F.col("type_id"),
        F.col("project_service_line_id").alias("service_line_id")
    ).dropna(how='any') # Any cause we don't need a row that gives just and Id from another table.
    return df



@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.activity_department",
    comment=(
        "Raw activity data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_activity_department_table():
    raw = spark.read.option("multiLine", True)\
                        .json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/app_timesheet_report_anonymized.json")

    # They are already unpacked so no need to analyze anything, just
    # select the type_id and service_line
    df = raw.select(
        F.col("type_id"),
        F.col("project_department_id").alias("department_id")
    ).dropna(how='any')
    return df
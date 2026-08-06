import pyspark.pipelines as dp
import pyspark.sql.functions as F
import pysparl.sql.types as ty
from constants import CATALOG, TARGET_SCHEMA, READ_SCHEMA
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)

@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.worker",
    comment=(
        "Raw worker data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def create_worker_table():

    
    # We load the json
    raw = spark.read.option("multiLine", True).json(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/perso_workers_anonymized.json")
    # We let it infer the schema by itself and we select what we want

    df = raw.select(
        F.col("id"), # Must be string cause IDs are strings
        F.col("active").astype("boolean"),
        F.to_date(F.col("start_date"), "dd/MM/yyyy"),
        F.col("mail"),
        F.to_date(F.col("seniority_date"), "dd/MM/yyyy"),
        F.col("job_title"),
        F.col("fulltime_or_parttime"),
        F.col("productivity_coefficient").cast("float"), # It is between 0 and 1
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

    BASE_STR = """
    {
        amount_in_euros: null,
        amount_in_local_currency: null,
        local_currency: null,
        local_currency_conv: null,
        local_currency_rate: null
    }
    """.replace("\n", "").replace("\t", "")

    BASE_STRUCT = ty.StructType([
        ty.StructField("amount_in_euros", ty.IntegerType(), nullable=True),
        ty.StructField("amount_in_local_currency", ty.IntegerType(), nullable=True),
        ty.StructField("local_currency", ty.StringType(), nullable=True),
        ty.StructField("local_currency_conv", ty.FloatType(), nullable=True),
        ty.StructField("local_currency_rate", ty.FloatType(), nullable=True)
    ])

    df = df.withColumn(
        "tariff",
        F.regexp_replace(F.col("tariff"), "[]", BASE_STR)
    )

    df = df.withColumn(
        "tariff",
        F.from_json(F.col("tariff"), BASE_STRUCT)
    )
    return df
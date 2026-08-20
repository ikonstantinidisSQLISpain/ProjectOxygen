import pyspark.pipelines as dp
import pyspark.sql.functions as F
import pyspark.sql.types as ty
from src.flow.pipelines.app.bronze.constants import CATALOG, TARGET_SCHEMA, READ_SCHEMA
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)

@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.worklogs",
    comment=(
        "Raw worklogs data, formatted"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def load_worklogs():

    path = f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/onetbp_worklogs_anonymized.json"

    schema = ty.StructType([
        ty.StructField("uid", ty.StringType(), False),
        ty.StructField(
            "worklogs",
            ty.MapType(
                ty.StringType(),
                ty.MapType(
                    ty.StringType(),
                    ty.VariantType()
                )
            ),
            False),
        ty.StructField("available_date", ty.StringType(), False),
        ty.StructField(
            "worklogs_by_type",
            ty.MapType(
                ty.StringType(),
                ty.MapType(
                    ty.StringType(),
                    ty.VariantType()
                )
            ),
            False)
    ])

    raw = spark.read.option("multiLine", True).json(path, schema=schema)

    worklogs = raw.select(
        F.col("uid"),
        F.explode(F.col("worklogs")).alias("date", "worklog_value"),
        F.col("available_date"),
    )

    # Must be splitted and joined later to prevent possible mismatches.
    worklogs_type = raw.select(
        F.col("uid"),
        F.col("available_date"),
        F.explode(F.col("worklogs_by_type")).alias("date", "type"),
    )


    # We expand the JSON data.
    worklogs_2 = worklogs.select(
        F.col("uid"),
        F.col("date"),
        F.explode(F.col("worklog_value")).alias("number", "subdata"),
        F.col("available_date"),
    )

    worklogs_type_2 = worklogs_type.select(
        F.col("uid"),
        F.col("available_date"),
        F.col("date"),
        F.explode(F.col("type")).alias("number", "subdata"),
    )


    
    schema_type = ty.StructType([
            ty.StructField("abscence", ty.StringType(), True),
            ty.StructField("project", ty.StringType(), True),
        ])

    worklogs_type_2 = worklogs_type_2.select(
        F.col("uid"),
        F.col("available_date"),
        F.col("date"),
        F.col("number"),
        F.col("subdata").cast(schema_type),
    )

    # We finish the worklogs_type by expanding the last json
    worklogs_type_3 = worklogs_type_2.select(
        F.col("uid"),
        F.col("available_date"),
        F.col("date"),
        F.col("number"),
        F.col("subdata.abscence").alias("abscence"),
        F.col("subdata.project").alias("project"),
    )



    # Worklogs follows a strange pattern where some values are a list and some values
    # are dictionary so we split it using this ruele

    worklogs_sub_1 = worklogs_2.where("cast(subdata AS STRING) LIKE '{%'")
    worklogs_sub_2 = worklogs_2.where("cast(subdata AS STRING) LIKE '[%'")


    # We expand the array like subset
    worklogs_sub_2 = worklogs_sub_2.select(
        F.col("uid"),
        F.col("date"),
        F.col("number"),
        F.posexplode( # posexplode adds the index as column
            F.col("subdata").astype(
                ty.ArrayType(ty.VariantType())
            )
        ).alias("index", "subdata"),
        F.col("available_date"),
        F.col("date")
    )

    # And cast it to struct
    schema_subdata = ty.StructType([
        ty.StructField("type", ty.StringType(), True),
        ty.StructField("tbp_id", ty.StringType(), True),
        ty.StructField("worklog", ty.StringType(), True),
        ty.StructField("project_code", ty.StringType(), True),
        ty.StructField("project_name", ty.StringType(), True),
    ])

    worklogs_sub_2 = worklogs_sub_2.select(
        F.col("uid"),
        F.col("date"),
        F.col("number"),
        F.col("index"),
        F.col("subdata").cast(schema_subdata),
        F.col("available_date"),
        F.col("date"),
    )

    # We set the last array as columns
    schema_2 = ty.MapType(
        ty.StringType(),
        schema_subdata
    )
    # Same subset 1
    worklogs_sub_1 = worklogs_sub_1.select(
        F.col("uid"),
        F.col("date"),
        F.col("number"),
        F.explode(F.col("subdata").cast(schema_2)).alias("number_2", "subdata"),
        F.col("available_date"),
        F.col("date"),
    )

    worklogs_sub_1 = worklogs_sub_1.select(
        F.col("uid"),
        F.col("date"),
        F.col("number"),
        F.col("number_2"),
        F.col("subdata.type").alias("worklog_type"),
        F.col("subdata.tbp_id").alias("tbp_id"),
        F.col("subdata.worklog").alias("worklog"),
        F.col("subdata.project_code").alias("project_code"),
        F.col("subdata.project_name").alias("project_name"),
        F.col("available_date"),
    )

    

    worklogs_sub_2 = worklogs_sub_2.select(
        F.col("uid"),
        F.col("date"),
        F.col("number"),
        F.col("index"),
        F.col("subdata.type").alias("worklog_type"),
        F.col("subdata.tbp_id").alias("tbp_id"),
        F.col("subdata.worklog").alias("worklog"),
        F.col("subdata.project_code").alias("project_code"),
        F.col("subdata.project_name").alias("project_name"),
        F.col("available_date"),
    )


    # Since they are the same dataset but splitted by rows, we will join them back in

    worklogs_final = worklogs_sub_1.unionByName(worklogs_sub_2, allowMissingColumns=True)

    # lastly we add the worklogs_by_type data
    # by joining using the uid, date, number,
    # and available date cols
    final = worklogs_final.join(
        worklogs_type_3,
        on=["uid", "date", "number", "available_date"],
        how="full"
    )

    final = final.select(
        F.col("uid").alias("worker_id"),
        F.col("date").alias("year_month"),
        F.col("number").alias("day"),
        F.to_date(F.col("available_date"), "yyyy-MM-dd").alias("available_date"),
        F.col("number_2"),
        F.col("worklog_type"),
        F.col("tbp_id"),
        F.col("worklog").cast("float").alias("workload"),
        F.col("project_code"),
        F.col("project_name"),
        F.col("index"), # Already int
        F.col("abscence").cast("float"),
        F.col("project").cast("float")
    )

    return final
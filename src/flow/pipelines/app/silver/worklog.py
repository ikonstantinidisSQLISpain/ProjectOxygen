import pyspark.pipelines as dp
import pyspark.sql.functions as F
import sys, os
sys.path.append(os.path.abspath('./src/flow/pipelins/app/silver/constants.py'))
from src.flow.pipelines.app.silver.constants import CATALOG, TARGET_SCHEMA, READ_SCHEMA, METADATA_COLUMNS
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)


@dp.materialized_view(
    name=f"{CATALOG}.{TARGET_SCHEMA}.worklogs",
    comment=(
        "Collaboration Status, Date added as date"
    ),
    table_properties={
        "quality":"silver"
    }
)
def add_date_col():
    bronze = spark.read.table(f"{CATALOG}.{READ_SCHEMA}.worklogs")

    df = bronze.withColumn(
        "date",
        F.to_date(
            F.concat_ws("/", F.col("day"), F.col("year_month")),
            "dd/MM/yyyy"
        )
    )

    for c in METADATA_COLUMNS:
        df = df.drop(c)
    return df
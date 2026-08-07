import pyspark.pipelines as dp
import pyspark.sql.functions as F
from constants import CATALOG, TARGET_SCHEMA, READ_SCHEMA
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)


@dp.materialized_view(
    name=f"{CATALOG}.{TARGET_SCHEMA}.timesheet",
    comment=(
        "Timesheet, Date added as date"
    ),
    table_properties={
        "quality":"silver"
    }
)
def add_date_col():
    bronze = spark.read.table(f"{CATALOG}.{READ_SCHEMA}.timesheet")

    df = bronze.withColumn(
        "date",
        F.make_date(
            F.col("year"),
            F.col("month"),
            F.lit(1)
        )
    )
    return df
import pyspark.sql as sql
import pyspark.pipelines as dp
import pyspark.sql.functions as F
from bronze_constants import CATALOG, TARGET_SCHEMA, KNOWN_SCHEMAS_HISTORY, EXLCUDE_HISTORY_COLUMNS
CATALOG, TARGET_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark)


def remove_hashed_cols(list_of_cols):
    "variant cols have a hash equivalent, the variant cols cant be accounted for when using dropDuplicates, so we remove those cols from the subset of cols to dropDuplicates"

    cols_with_hash = list()
    for c in list_of_cols:
        if c.startswith("hash_"):
            cols_with_hash.append(c[len("hash_"):])

    cols = [c for c in list_of_cols if c not in cols_with_hash]

    return cols


def last_snapshot_filter(snapshot_df, partition_cols, snapshot_ts_col, extra_cols_to_skip=None):

    # First we drop duplicates on all columns except the snapshot_dates one
    cols = snapshot_df.columns

    if extra_cols_to_skip is None:
        extra_cols_to_skip = list()

    if not isinstance(extra_cols_to_skip, list):
        raise ValueError("extra cols must be a list.")

    if partition_cols not in cols:
        raise ValueError(f"partition column ({snapshot_ts_col}) is not in columns.")

    if snapshot_ts_col not in cols:
        raise ValueError(f"snapshot timestamp column ({snapshot_ts_col}) is not in columns.")
    
    cols_without_snapshot_ts = [c for c in cols if c != snapshot_ts_col and c not in extra_cols_to_skip]

    newdf = snapshot_df.dropDuplicates(subset=remove_hashed_cols(cols_without_snapshot_ts))

    window = sql.Window.partitionBy(partition_cols).orderBy(snapshot_ts_col)

    newdf = newdf.withColumn(
        "last_snapshot",
        F.max(
            F.col(snapshot_ts_col)
        ).over(window)
    ).withColumn(
        "row_number", 
        F.row_number().over(window)
    ).filter(
        (F.col(snapshot_ts_col) == F.col("last_snapshot"))
        & (F.col("row_number") == 1) # We choose 1 cause it should not matter if it is the first or last, and we won't bother calculating the last
    ).drop("row_number").drop("last_snapshot")


    return newdf



def pipe_maker(table):

    @dp.materialized_view(
        name=f"{CATALOG}.{TARGET_SCHEMA}.current_{table}"
    )
    def f():
        df = spark.read.table(f"{CATALOG}.{TARGET_SCHEMA}.base_{table}")
        return last_snapshot_filter(df, "id", "snapshot_ts", ["remaining_payload"])

    return None


for table in KNOWN_SCHEMAS_HISTORY.keys():
    pipe_maker(table)
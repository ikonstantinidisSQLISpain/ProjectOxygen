

import pyspark.sql as sql
import pyspark.sql.functions as F
import pyspark.pipelines as dp
import common.constants as c
import common.utils as ut
from pathlib import Path

def remove_hashed_cols(list_of_cols):
    "variant cols have a hash equivalent, the variant cols cant be accounted for when using dropDuplicates, so we remove those cols from the subset of cols to dropDuplicates"

    cols_with_hash = list()
    for c in list_of_cols:
        if c.startswith("hash_"):
            cols_with_hash.append(c[len("hash_"):])

    cols = [c for c in list_of_cols if c not in cols_with_hash]

    return cols




def last_snapshot_filter(snapshot_df, partition_cols, extra_cols_to_skip=None):

    # Extra cols to skip, cols that dont want to include in subset during dropDuplicates
    snapshot_ts_col = c.SNAPSHOT_COL
    # First we drop duplicates on all columns except the snapshot_dates one
    cols = snapshot_df.columns

    if extra_cols_to_skip is None:
        extra_cols_to_skip = list()

    if not isinstance(extra_cols_to_skip, list):
        raise TypeError("extra cols must be a list.")

    if not isinstance(partition_cols, (str, list)):
        raise TypeError("partition cols must be a list or a str")

    if isinstance(partition_cols, str):
        partition_cols = [partition_cols]

    if not ut.all_in(partition_cols, cols):
        raise ValueError(f"partition columns ({partition_cols}, {cols}) are not in columns.")

    if snapshot_ts_col not in cols:
        raise ValueError(f"snapshot timestamp column ({snapshot_ts_col}) is not in columns.")

    
    cols_without_snapshot_ts = [c for c in cols if c != snapshot_ts_col and c not in extra_cols_to_skip]

    newdf = snapshot_df.dropDuplicates(subset=remove_hashed_cols(cols_without_snapshot_ts))

    window = sql.Window.partitionBy(*partition_cols).orderBy(snapshot_ts_col)

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


def history_table_maker(snapshot_df, cols_to_keep, dates_filter_expr, history_extra_constraints):

    cols = snapshot_df.columns

    ndf = snapshot_df
    if history_extra_constraints is not None:
        if not isinstance(history_extra_constraints, list):
            raise TypeError("history_extra_constraints must be a list of sql constraints")
        for con in history_extra_constraints:
            ndf = ndf.where(con)

    if not isinstance(cols_to_keep, list):
        raise ValueError("columns to keep must be a list.")

    if not ut.all_in(cols_to_keep, cols):
        raise ValueError(f"cols to keep ({cols_to_keep}, {cols}) are not in columns.")

    cols_to_keep_2 = list(cols_to_keep)
    cols_to_keep_2.append(c.SNAPSHOT_COL)
    ndf = ndf.where(dates_filter_expr.replace("snapshot_ts", c.SNAPSHOT_COL).replace("latest_snapshot_date", c.KNOWN_SNAPSHOT)).select(*cols_to_keep_2)

    return ndf


def to_list(val):
    if isinstance(val, list):
        return val
    return [val]


def create_latest_batch_stream(
    source_table: str,
    target_table: str,
    checkpoint_path: str
):
    source_df = spark.readStream.table(source_table)

    def process_batch(batch_df, batch_id):
        (
            batch_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target_table)
        )

    return (
        source_df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", checkpoint_path)
            .start()
    )


def snapshot_pipe_maker(spark, catalog, platform, table_name):

    platform_name = ut.get_platform(spark, platform)
    target_quality = ut.get_quality(spark, "bronze")
    target_schema = ut.schema_name_builder(target_quality, platform_name)

    read_table = ut.table_name_builder("base", table_name)
    current_table = ut.table_name_builder("current", table_name)
    history_table = ut.table_name_builder("history", table_name)
    read_path = f"{catalog}.{target_schema}.{read_table}"
    current_table_path = f"{catalog}.{target_schema}.{current_table}"
    history_table_path = f"{catalog}.{target_schema}.{history_table}"

    dates_filter_expr = ut.get_conf(spark, "history.filter_expression")

    default_config_file_name = ut.get_conf(spark, "file.default_config")
    table_config_file_name = ut.get_table_metadata_files(spark, "config", platform, table_name)
    table_config_volume = ut.get_table_volume(spark, "config", platform, table_name)
    table_config_path = Path(table_config_volume) / table_config_file_name
    default_config_path = Path(table_config_volume) / default_config_file_name
    table_config = ut.get_table_config(table_config_path, platform, table_name, default_config_path)
    default_config = ut.load_json(default_config_path)


    """
    dp.create_streaming_table(
        name=current_table_path,
        comment=f"Last snapshot {table_name} data.",
        table_properties={
            "quality": target_quality
        },
        expect_all_or_drop={"not_null_snapshot_date": f"{c.SNAPSHOT_COL} IS NOT NULL"}
    )
    dp.create_auto_cdc_flow(
        target=current_table_path,
        source=read_path,
        keys=to_list(table_config.get("last_snapshot", default_config["last_snapshot"])["partition_cols"]),
        sequence_by=c.SNAPSHOT_COL
    )
    """

    """
    @dp.materialized_view(
        name=current_table_path,
        comment=f"Last snapshot {table_name} data.",
        table_properties={
            "quality": target_quality
        }
    )
    @dp.expect_or_drop("not_null_snapshot_date", f"{c.SNAPSHOT_COL} IS NOT NULL")
    def f():
        df = spark.readStream.table(read_path).where(f"{c.SNAPSHOT_COL} IS NOT NULL")
        current_df = ut.max_filter(
            df, 
            table_config.get("last_snapshot", default_config["last_snapshot"]).get("partition_cols"), 
            c.SNAPSHOT_COL,
            current_table_path.replace(".","_"))
        return current_df
    """
    
    @dp.table(
        name=current_table_path,
        comment=f"Last snapshot {table_name} data.",
        table_properties={
            "quality": target_quality
        }
    )
    @dp.expect_or_drop("not_null_snapshot_date", f"{c.SNAPSHOT_COL} IS NOT NULL")
    def f():
        t_name = f"{catalog}.{platform}.{table_name}_temp"
        df = spark.readStream.table(read_path).where(f"{c.SNAPSHOT_COL} IS NOT NULL")#.createOrReplaceTempView(t_name)
        """current_df = spark.sql(f'''
        SELECT * FROM {t_name} AS t WHERE t.{c.SNAPSHOT_COL} > t.{c.KNOWN_SNAPSHOT} OR (
            t.{c.SNAPSHOT_COL} = t.{c.KNOWN_SNAPSHOT}
            AND NOT EXISTS (
                SELECT 1
                FROM {t_name} AS t2
                WHERE t2.{c.SNAPSHOT_COL} > t2.{c.KNOWN_SNAPSHOT}
        );
        
        ''')"""
        
        return df.where(f"{c.SNAPSHOT_COL} = {c.KNOWN_SNAPSHOT}") # Since the job is executed before the pipeline, it will always have the latest date stored, assuming the date in the file name is correct.
    
    """
    def process_batch(batch_df, batch_id):
        batch_df = batch_df.filter(
            f"{c.SNAPSHOT_COL} IS NOT NULL"
        )

        batch_df.createOrReplaceTempView("current_batch")

        current_df = spark.sql(f'''
            SELECT *
            FROM current_batch AS t
            WHERE t.{c.SNAPSHOT_COL} > t.{c.KNOWN_SNAPSHOT}
                OR (
                    t.{c.SNAPSHOT_COL} = t.{c.KNOWN_SNAPSHOT}
                    AND NOT EXISTS (
                        SELECT 1
                        FROM current_batch AS t2
                        WHERE t2.{c.SNAPSHOT_COL} > t2.{c.KNOWN_SNAPSHOT}
                    )
                )
        ''')
        # Aquí haces el write/merge que corresponda
        current_df.write.mode("append").saveAsTable(target_table)
        return None

    query = (
        spark.readStream
            .table(read_path)
            .writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", checkpoint_path)
            .start()
    )
    """
    #create_latest_batch_stream(read_path, current_table_path, "/Volumes/oxygen_dev/landing/source/_checkpoints_write/")

    if table_config.get("history", None) is not None:
        history_prev_filters = table_config.get("history").get("history_extra_constraints", None)
        @dp.table(
            name=history_table_path,
            comment=f"Required time series {table_name} data.",
            table_properties={
                "quality": target_quality
            }
        )
        @dp.expect_or_drop("not_null_snapshot_date", f"{c.SNAPSHOT_COL} IS NOT NULL")
        def f2():
            df = spark.readStream.table(read_path)
            history_df = history_table_maker(df, table_config["history"]["cols_to_keep"], dates_filter_expr, history_prev_filters)
            return history_df

    return None
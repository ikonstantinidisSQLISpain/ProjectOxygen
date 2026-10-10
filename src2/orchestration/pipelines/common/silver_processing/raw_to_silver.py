import pyspark.pipelines as dp
import pyspark.sql.functions as F
import pyspark.sql.types as ty
import common.utils as ut
import common.constants as c
import common.raw_processing.payload_to_table as pt
import common.raw_processing.snapshot_processor as sp
import common.silver_processing.basic_filtering_and_quarantine as bfq
from pathlib import Path


def from_raw_to_full_silver_steps(spark, table_name, raw_df):

    known_schema_vol = ut.get_table_volume(spark, "schema", platform, table_name)
    known_schema_file = ut.get_table_metadata_files(spark, "schema", platform, table_name)
    known_schema_path = Path(known_schema_vol) / Path(known_schema_file)
    known_latest_snapshot = ut.load_json(ut.get_conf(spark, "file.latest_snapshot")).get(platform).get(table_name)
    known_schema = ut.load_json(known_schema_path)
    metadata_cols = c.METADATA_COLUMNS

    base = pt.transform_payload_to_table(raw_df, known_schema, metadata_cols)
    base = base.withColumn(c.KNOWN_SNAPSHOT, F.lit(known_latest_snapshot))
    base = ut.trim_str_cols(base)

    if table_name == "talent":
        base = ut.add_seniority(base, "yearsOfExperience")
        base = ut.add_date_status(base, "lastConnectionDate")
    if table_name == "collab_status":
        base = ut.classify_status_code(spark, base, "status")
    if table_name == "skill":
        base = ut.extract_all_english_terms(spark, base)
    if table_name == "profile":
        base = ut.add_profile_score_f(base)
        base = ut.add_bucket_and_status(base, "completionRate")
        base = ut.add_date_status_prof(base, "lastModifiedDate")
    if table_name in ["certification"]:
        base = ut.add_is_active_col(base, "endDate")

    default_config_file_name = ut.get_conf(spark, "file.default_config")
    table_config_file_name = ut.get_table_metadata_files(spark, "config", platform, table_name)
    table_config_volume = ut.get_table_volume(spark, "config", platform, table_name)
    table_config_path = Path(table_config_volume) / table_config_file_name
    default_config_path = Path(table_config_volume) / default_config_file_name
    table_config = ut.get_table_config(table_config_path, platform, table_name, default_config_path)
    silver_config = table_config.get("silver", None)

    cols_to_drop = silver_config.get("cols_to_drop", None)
    drop_metadata = silver_config.get("drop_metadata", True)
    filter_queries = silver_config.get("constraints", None)
    variant_keys_to_extract = silver_config.get("variant_keys_to_extract", None)
    quarantine_queries = silver_config.get("quarantine", None)
    middle_table = silver_config.get("middle_table", None)
    pk_cols = silver_config.get("pk_cols", None)

    if variant_keys_to_extract is not None:
        if not isinstance(variant_keys_to_extract, dict):
            raise TypeError("variant keys to extract must be a dict. Col: Dict[key, format]")
        for col, keys_format in variant_keys_to_extract.items():
            silver_df = ut.extract_variant_keys(silver_df, col, keys_format)

    if filter_queries is not None:
        silver_df = ut.filter_conditions(silver_df, filter_queries)


    return silver_df





def get_last_snapshot(silver_df):

    current_df = silver_df.where(f"{c.SNAPSHOT_COL} = {c.KNOWN_SNAPSHOT}")

    return current_df

def get_history(silver_df, cols_to_keep, dates_filter_expr, history_extra_constraints):
    history_df = sp.history_table_maker(silver_df, cols_to_keep, dates_filter_expr, history_extra_constraints)
    return history_df




def bronze_to_silver_pipe_maker(spark, catalog, platform, table_name):

    platform_name = ut.get_platform(spark, platform)
    
    read_quality = ut.get_quality(spark, "bronze")
    read_schema = ut.schema_name_builder(read_quality, platform_name)
    target_quality = ut.get_quality(spark, "silver")
    target_schema = ut.schema_name_builder(target_quality, platform_name)

    read_table = ut.table_name_builder("raw", table_name)
    target_table = ut.table_name_builder("cleaned", table_name)
    read_path = f"{catalog}.{read_schema}.{read_table}"
    target_path = f"{catalog}.{target_schema}.{target_table}"

    default_config_file_name = ut.get_conf(spark, "file.default_config")
    table_config_file_name = ut.get_table_metadata_files(spark, "config", platform, table_name)
    table_config_volume = ut.get_table_volume(spark, "config", platform, table_name)
    table_config_path = Path(table_config_volume) / table_config_file_name
    default_config_path = Path(table_config_volume) / default_config_file_name
    table_config = ut.get_table_config(table_config_path, platform, table_name, default_config_path)
    silver_config = table_config.get("silver", None)

    if silver_config is not None:
        cols_to_drop = silver_config.get("cols_to_drop", None)
        drop_metadata = silver_config.get("drop_metadata", True)
        quarantine_queries = silver_config.get("quarantine", None)
        middle_table = silver_config.get("middle_table", None)
        pk_cols = silver_config.get("pk_cols", None)
        if pk_cols is None:
            raise ValueError(f"PK Columns must be provided in table quality config. It is missing in the json file for {table_name}.")
        # We add the snapshot col
        if c.SNAPSHOT_COL not in pk_cols:
            pk_cols.append(c.SNAPSHOT_COL)


        @dp.table(
            name=target_path,
            comment=f"{table_name} cleaned and filtered data.",
            table_properties={
                "quality": target_quality
            }
        )
        def f():
            df = spark.readStream.table(read_path)
            df = from_raw_to_full_silver_steps(spark, table_name, df)
            return df

        if quarantine_queries is not None:
            ut.create_quarantine_table_sanitize(spark, catalog)
            quarantine_schema = ut.get_quarantine_schema(spark)
            quarantine_table = ut.get_quarantine_table(spark, "sanitize")
            quarantine_path = f"{catalog}.{quarantine_schema}.{quarantine_table}"
            @dp.append_flow(
                target=quarantine_path,
                name=f"flow_{target_path.replace('.','_')}_{quarantine_path.replace('.','_')}"
            )
            def f2():
                df = spark.readStream.table(read_path)
                return ut.make_quarantine_table(df, quarantine_queries, pk_cols, read_path)

        bfq.middle_table_pipe_maker(spark, catalog, target_path, target_schema, target_quality, table_name, middle_table_config, add_snapshot_col=True)

    return None




def silver_to_gold(spark, site_df):

    dim_site = ut.map_site(spark, site_df)
    dim_collab

    return None











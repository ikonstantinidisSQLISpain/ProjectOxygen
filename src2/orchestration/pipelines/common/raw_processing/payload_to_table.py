

import pyspark.pipelines as dp
import pyspark.sql.functions as F
import pyspark.sql.types as ty
import common.utils as ut
import common.constants as c
from pathlib import Path
# This file has all the functions neccessary to translate a payload variant column from raw table into a bronze table.

#import cloudpickle
#cloudpickle.register_pickle_by_value(sys.modules[__name__])


def payload_subset_expr_string_maker(variant_col_name, cols_to_select):
    expr_string = "object_construct("
    n_cols = len(cols_to_select)
    for i, col in enumerate(cols_to_select):
        if i+1 == n_cols:
            end_str = ""
        else:
            end_str = ","
        expr_string = expr_string + f"'{col}', {variant_col_name}:{col}{end_str}"

    expr_string = expr_string + ")"
    return expr_string


def parse_data_type(dt_):
    if dt_ == "DATE":
        return "STRING"
    return dt_

@F.udf(returnType=ty.ArrayType(ty.StringType()))
def get_variant_keys(variant):
    if variant is None:
        return None

    value = variant.toPython()

    if isinstance(value, dict):
        return list(value.keys())

    return list()


def transform_payload_to_table(og_df, known_schema, metadata_cols):
    """
    Takes the og_df that is meant to have only payload column and metadata.

    Takes known_schema that is a dict with the shape (column: {type: str, format: str})

    Selects the known schema columns and transform it to columns, and the remaining payload json is kept in the column remaining_payload
    """
    cols = list()

    for field_name, data_type in known_schema.items():
        if data_type["type"] == "VARIANT":
            col = (
                F.parse_json(F.expr(f"variant_get(payload, '$.{field_name}', 'string')"))
                .cast(parse_data_type(data_type["type"]))
                .alias(field_name)
            )
        else:
            col = (
                F.expr(f"variant_get(payload, '$.{field_name}', 'string')")
                .cast(parse_data_type(data_type["type"]))
                .alias(field_name)
            )
        cols.append(col)
        """
        if data_type["type"] == "VARIANT":
            col2 = (
                F.expr(f"xxhash64(variant_get(payload, '$.{field_name}', 'string'))")
                .cast(parse_data_type("STRING"))
                .alias(f"hash_{field_name}")
            )
            cols.append(col2)
        """

    cols.extend(metadata_cols)
    cols.append(c.SNAPSHOT_COL)
    cols.append(c.INGEST_TS_COL)

    df = og_df.withColumn(
            "remaining_keys",
            F.array_except(
                get_variant_keys("payload"),
                F.array(*[F.lit(k) for k in known_schema.keys()])
            )
        ).withColumn(
            "remaining_payload",
            F.expr("""
                to_variant_object(
                    map_from_arrays(
                        transform(
                            remaining_keys,
                            k -> k
                        ),
                        transform(
                            remaining_keys,
                            k -> variant_get(payload, '$.k', 'string')
                        )
                    )
                )
            """)
        )

    cols.append("remaining_payload")
    return df.select(*cols)




def payload_top_level_to_table_pipe_maker(spark, catalog, platform, table_name):

    platform_name = ut.get_platform(spark, platform)
    target_quality = ut.get_quality(spark, "bronze")
    target_schema = ut.schema_name_builder(target_quality, platform_name)

    read_table = ut.table_name_builder("raw", table_name)
    target_table = ut.table_name_builder("base", table_name)
    read_path = f"{catalog}.{target_schema}.{read_table}"
    target_path = f"{catalog}.{target_schema}.{target_table}"
    known_schema_vol = ut.get_table_volume(spark, "schema", platform, table_name)
    known_schema_file = ut.get_table_metadata_files(spark, "schema", platform, table_name)
    known_schema_path = Path(known_schema_vol) / Path(known_schema_file)

    known_latest_snapshot = ut.load_json(ut.get_conf(spark, "file.latest_snapshot")).get(platform).get(table_name)

    @dp.table(
        name=target_path,
        comment=f"Base Bronze {table_name}. Contains the top level payload keys as columns and formatted when possible.",
        table_properties={
            "quality":target_quality
        }
    )
    def f():
        df = spark.readStream.table(read_path)
        known_schema = ut.load_json(known_schema_path)
        metadata_cols = c.METADATA_COLUMNS
        if table_name == "profile":
            ndf = ut.add_bucket_and_status(
                ut.add_profile_score_f(
                    transform_payload_to_table(df, known_schema, metadata_cols)
                ), 
                "completionRate").withColumn(c.KNOWN_SNAPSHOT, F.lit(known_latest_snapshot))
            return ndf
        ndf = transform_payload_to_table(df, known_schema, metadata_cols).withColumn(c.KNOWN_SNAPSHOT, F.lit(known_latest_snapshot))
        return ndf

    return None
import pyspark.pipelines as dp
import pyspark.sql.functions as F
import pyspark.sql.types as ty
from bronze_constants import CATALOG, READ_SCHEMA, TARGET_SCHEMA, METADATA_COLUMNS, KNOWN_SCHEMAS_PATHS
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)





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
        col = (
            F.expr(f"variant_get(payload, '$.{field_name}', 'string')")
            .cast(parse_data_type(data_type["type"]))
            .alias(field_name)
        )
        cols.append(col)
        if data_type["type"] == "VARIANT":
            col2 = (
                F.expr(f"xxhash64(variant_get(payload, '$.{field_name}', 'string'))")
                .cast(parse_data_type("STRING"))
                .alias(f"hash_{field_name}")
            )
            cols.append(col2)

    cols.extend(metadata_cols)

    df = og_df.filter(F.col("file_name_error").isNull()).withColumn(
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



"""This phase simply takes the payload and takes the top level schema"""
def pipe_maker(table_name):


    @dp.materialized_view(
        name=f"{CATALOG}.{TARGET_SCHEMA}.base_{table_name}"
    )
    def f():
        table = spark.read.table(f"{CATALOG}.{TARGET_SCHEMA}.raw_{table_name}")
        return transform_payload_to_table(table, KNOWN_SCHEMAS_PATHS[table_name], METADATA_COLUMNS)


    # We add the derivation to quarantine data
    @dp.materialized_view(

    )

    return None


for table in KNOWN_SCHEMAS_PATHS.keys():
    pipe_maker(table)



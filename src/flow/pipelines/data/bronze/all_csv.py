import os
from pyspark.sql import functions as F
import pyspark.pipelines as dp


CATALOG = spark.conf.get("read.catalog")
TARGET_SCHEMA = spark.conf.get("bronze.schema")
READ_SCHEMA = spark.conf.get("raw.schema")




def clean_col_name(col_name):
    new_name = col_name.strip().replace(" ", "_").lower()
    new_name = new_name.replace("(", "").replace(")", "")
    return new_name


def get_name_and_date(file_name):
    split = file_name.split("_")
    try:
        name = split[0]
        month = split[1]
        year = split[2]
    except IndexError:
        name, month, year = None, None, None
    return name, month, year

def load_csv_data(name):
    all_files = os.listdir(f"/Volumes/{CATALOG}/{READ_SCHEMA}/source")
    selected_files = [file for file in all_files if get_name_and_date(file)[0] == name]
    raw_list = [
        spark.read.csv(
            f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/{file}",
            header=True, sep=";")
        for file in selected_files]

    df_list = [
        df.toDF(
            *[clean_col_name(c) for c in df.columns]
            ).select(
                "*",
                F.col("_metadata.file_path").alias("_source_file"),
                F.col("_metadata.file_name").alias("_source_file_name"),
                F.col("_metadata.file_size").alias("_source_file_size"),
                F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            ) for df in raw_list
    ]

    # It is assumed that all files have the same columns
    final = df_list[0]
    for i in range(1, len(df_list)):
        final = final.union(df_list[i])
    

    return final


def pipe_builder(name):

    @dp.table(
        name=f"{CATALOG}.{TARGET_SCHEMA}.{name}",
        comment=(
        f"Raw {name} data, formatted"
        ),
        table_properties={
            "quality":"bronze"
        }
    )
    def f():
        return load_csv_data(name)

    return None


for table in ["opportunities", "orders"]:
    pipe_builder(table)
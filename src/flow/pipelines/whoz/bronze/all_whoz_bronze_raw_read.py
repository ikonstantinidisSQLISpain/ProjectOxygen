import pyspark.pipelines as dp
import pyspark.sql.functions as F
from pathlib import Path
from bronze_constants import CATALOG, TARGET_SCHEMA, READ_SCHEMA
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)
import pyspark.sql.types as ty



class RawReader(): # Hardcoded everywhere cause imports dont work

    @staticmethod
    def read_json(sparkSession,
                  catalog: str,
                  read_schema: str,
                  file_name: str):
        
        path = f"/Volumes/{catalog}/{read_schema}/source/{file_name}.json"
        df = sparkSession.read.option("multiLine", True).json(path)
        return df

    @staticmethod
    def volume_path_maker(catalog: str, read_schema: str, volume_name: str):
        return Path(f"/Volumes/{catalog}/{read_schema}/{volume_name}/")

    @staticmethod
    def get_date_and_error(file_name: str):
        """
        Extracts the snapshot date if possible and an error description if it is not.

        Returns a tuple, first value is date or None, second is a string or None.
        """

        if not isinstance(file_name, str):
            return (None, "File name is not a string.")

        # Files are expected to have snapshot in the next format YYYY-MM-DD_file_name
        # We extract the first 10 character that are suppose to have a date.

        if len(file_name) < 11:
            return (None, "Date not provided.")

        date_str = file_name[:12]


        validity_count = 0
        n_chars = 11
        for i in range(n_chars + 1):
            char = date_str[i]
            if i in [4, 7]:
                if char != "-":
                    continue
            elif i == 11:
                if char != "_":
                    continue
            else:
                try:
                    int(char)
                except ValueError:
                    continue
            validity_count += 1

        if validity_count == n_chars:
            return (date_str[:10], None)
        elif validity_count < 12 and validity_count > 8:
            return (None, "Invalid date.")
        else:
            return (None, "Date not provided.")
        return None

    @staticmethod
    def raw_json_reader(sparkSession,
                        volume_path: str,
                        glob_regex: str,
                        streaming: bool = True):
        """Reads from volume expecting a list of jsons, 
        
        Returns a table with load metadata and a column named payload.
        
        The streaming table, considers a new input when a new file arrives.
        Does not bother checking changes.
        """

        inferred_schema_path = Path(volume_path) / '_checkpoints' / glob_regex.split(".")[0]

        if streaming:
            reader = sparkSession.readStream\
                                .format("cloudFiles")\
                                .option("cloudFiles.format", "json")\
                                .option("cloudFiles.schemaLocation", str(inferred_schema_path))
        else:
            reader = sparkSession.read.format("json")

        df = reader.option("multiLine", "true")\
                    .option("singleVariantColumn", "payload")\
                    .option("pathGlobFilter", glob_regex)\
                    .load(str(volume_path))\
                    .select(
                        "payload",
                        F.col("_metadata.file_path").alias("_source_file"),
                        F.col("_metadata.file_name").alias("_source_file_name"),
                        F.col("_metadata.file_size").alias("_source_file_size"),
                        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
                    ).lateralJoin(
                        sparkSession.tvf.variant_explode(F.col("payload").outer())
                    ).select(
                        F.col("value").alias("payload"),
                        "_source_file",
                        "_source_file_name",
                        "_source_file_size",
                        "_source_file_modified_at",
                    )

        # We add the snapshot date and the error if any as a column
        schema = ty.StructType([
            ty.StructField("date", ty.StringType(), True),
            ty.StructField("file_name_error", ty.StringType(), True),
        ])
        get_date = F.udf(RawReader.get_date_and_error, schema)
        df = (
            df
            .withColumn("resultado", get_date(F.col("_source_file_name")))
            .withColumn("snapshot_ts", F.to_date("resultado.date", "yyyy-MM-dd"))
            .withColumn("file_name_error", F.col("resultado.file_name_error"))
            .drop("resultado")
        )
        
        return df


def raw_pipe_maker(table_name, volume_path, files_glob_regex):


    @dp.table(
        name = f"{CATALOG}.{TARGET_SCHEMA}.raw_{table_name}",
        comment=(
            f"Raw {table_name}"
        ),
        table_properties={
            "quality":"bronze"
        }
    )
    def f():
        return RawReader.raw_json_reader(spark, volume_path, files_glob_regex, False)

    return None

BASE_VOL =  RawReader.volume_path_maker(CATALOG, READ_SCHEMA, "source")

WHOZ_DATA = {
    'certifications': (BASE_VOL, "*whoz__certification_report*.json"),
    "profiles": (BASE_VOL, "*whoz__profile_report*.json"),
    "skills": (BASE_VOL, "*whoz__skill_report*.json"),
    "users": (BASE_VOL, "*whoz__user_report*.json"),
    "talents": (BASE_VOL, "*whoz__talent_report*.json")
}

for name, (vol, regex) in WHOZ_DATA.items():
    raw_pipe_maker(name, vol, regex)
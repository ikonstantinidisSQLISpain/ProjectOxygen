import pyspark.pipelines as dp
import pyspark.sql.functions as F

from bronze_constants import CATALOG, READ_SCHEMA, TARGET_SCHEMA
from pathlib import Path
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)



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
                    ).withColumn(
                        'payload', 
                        F.col('payload').cast('ARRAY<STRING>')
                    ).select(
                        F.explode(F.col("payload")).alias("payload"),
                        "_source_file",
                        "_source_file_name",
                        "_source_file_size",
                        "_source_file_modified_at",
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
    'certifications': (BASE_VOL, "whoz__certification_report_anonymized.json"),
    "profiles": (BASE_VOL, "whoz__profile_report_anonymized.json"),
    "skills": (BASE_VOL, "whoz__skill_report_anonymized.json"),
    "users": (BASE_VOL, "whoz__user_report_anonymized.json"),
    "talents": (BASE_VOL, "whoz__talent_report_anonymized.json")
}

for name, (vol, regex) in WHOZ_DATA.items():
    raw_pipe_maker(name, vol, regex)
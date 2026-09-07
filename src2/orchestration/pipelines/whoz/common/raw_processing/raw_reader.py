import pyspark.pipelines as dp
import pyspark.sql.functions as F
from pathlib import Path



class RawReader(): # Hardcoded everywhere cause imports dont work

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


        if streaming:
            reader = sparkSession.readStream\
                                .format("cloudFiles")\
                                .option("cloudFiles.format", "json")\
            # No need to give spark permission to infer schema because,
            # we store the data as payload and we do it manually since spark's is not enough
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

def raw_pipe_maker(catalog, target_schema, table_name, volume_path, files_glob_regex, streaming=False):


    @dp.table(
        name = f"{catalog}.{target_schema}.raw_{table_name}",
        comment=(
            f"Raw {table_name}"
        ),
        table_properties={
            "quality":"bronze"
        }
    )
    def f():
        return RawReader.raw_json_reader(spark, volume_path, files_glob_regex, streaming)

    return None
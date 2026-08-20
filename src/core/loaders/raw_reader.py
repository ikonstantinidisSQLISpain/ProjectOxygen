
from pathlib import Path
import pyspark.sql.functions as F

class RawReader():

    @staticmethod
    def read_json(sparkSession,
                  catalog: str,
                  read_schema: str,
                  file_name: str):
        
        path = f"/Volumes/{catalog}/{read_schema}/source/{file_name}.json"
        df = sparkSession.read.option("multiLine", True).json(path)
        return df

    @staticmethod
    def volume_path_maker(catalog: str, read_schema: str):
        return Path(f"/Volumes/{catalog}/{read_schema}/source/")

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
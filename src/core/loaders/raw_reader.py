
from pathlib import Path

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
                        files_regex: str,
                        streaming: bool = True):
        """Reads from volume expecting a list of jsons, 
        
        Returns a table with load metadata and a column named payload.
        
        The streaming table, considers a new input when a new file arrives.
        Does not bother checking changes.
        """

        if streaming:
            reader = sparkSession.readStream
        else:
            reader = sparkSession.read

        inferred_schema_path = Path(volume_path) / '_checkpoints' / files_regex

        df = reader.format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("multiLine", "true")
                    .option("singleVariantColumn", "payload")
                    .option("cloudFiles.schemaLocation", inferred_schema_path)
                    .option("pathGlobFilter", files_regex)
                    .load(volume_path)
                    .select(
                        "payload",
                        F.col("_metadata.file_path").alias("_source_file"),
                        F.col("_metadata.file_name").alias("_source_file_name"),
                        F.col("_metadata.file_size").alias("_source_file_size"),
                        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
                    )
        return df
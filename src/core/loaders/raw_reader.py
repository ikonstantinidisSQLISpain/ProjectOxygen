

class RawReader():

    @staticmethod
    def read_json(sparkSession: pyspark.sql.SparkSession,
                  catalog: str,
                  read_schema: str,
                  file_name: str):
        
        path = f"/Volumes/{catalog}/{read_schema}/source/{file_name}.json"
        df = sparkSession.read.option("multiLine", True).json(path)
        return df
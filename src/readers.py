from pathlib import Path

class Reader():

    @staticmethod
    def read_json(path: str | Path):
        return spark.read.option("multiLine", True).json(path)
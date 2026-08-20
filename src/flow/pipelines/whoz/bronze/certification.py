import pyspark.pipelines as dp
import pyspark.sql.functions as F
from src.core.loaders.raw_reader import RawReader



@dp.table()
def read_certs():

    volume = RawReader.volume_path_maker(CATALOG, READ_SCHEMA)
    raw = RawReader.raw_json_reader(spark, volume, INFERED_SCHEMA_PATH, True)

    return raw
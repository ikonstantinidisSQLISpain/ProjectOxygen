import pyspark.pipelines as dp
import pyspark.sql.functions as F
from src.core.loaders.raw_reader import RawReader
from src.flow.pipelines.whoz.bronze.constants import CATALOG, READ_SCHEMA, TARGET_SCHEMA
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)



@dp.table(
    name = f"{CATALOG}.{TARGET_SCHEMA}.certifications",
    comment=(
        "Raw certifications"
    ),
    table_properties={
        "quality":"bronze"
    }
)
def read_certs():

    regex = "whoz__certifications_report_anonymized.json"
    volume = RawReader.volume_path_maker(CATALOG, READ_SCHEMA)
    raw = RawReader.raw_json_reader(spark, volume, regex, False)

    return raw
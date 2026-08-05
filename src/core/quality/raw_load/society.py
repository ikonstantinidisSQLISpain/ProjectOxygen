"""
The societies json contains simply the data from the original table
in JSON format.

To check that data from societies was loaded correctly, we have to
check that the number of distinct societies in the original data matches
both the number of distinct societies in the table which, at the same time
should match the number of rows.
"""

from src.core.loaders.raw_reader import RawReader

def check_societies_load(sparkSession: pyspark.sql.SparkSession,
                         catalog: str,
                         raw_schema: str,
                         societies_file_name: str):

    raw_soc_df = RawReader.read_json(sparkSession, catalog, raw_schema, societies_file_name)

    row_count = raw_soc_df.count()
    unique_societies_count = raw_soc_df.select("id").distinct().count()

    assert row_count == unique_societies_count
    return None
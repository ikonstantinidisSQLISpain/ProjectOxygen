"""
The entities json contains simply the data from the original table
in JSON format.

To check that data from entities was loaded correctly, we have to
check that the number of distinct entities in the original data matches
both the number of distinct entities in the table which, at the same time
should match the number of rows.
"""

from src.core.loaders.raw_reader import RawReader

def check_entities_load(sparkSession: pyspark.sql.SparkSession,
                        catalog: str,
                        raw_schema: str,
                        entities_file_name: str):

    raw_ent_df = RawReader.read_json(sparkSession, catalog, raw_schema, entities_file_name)

    row_count = raw_ent_df.count()
    unique_entities_count = raw_ent_df.select("id").distinct().count()

    assert row_count == unique_entities_count
    return None
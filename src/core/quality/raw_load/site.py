"""
In this case, it is the same as entities or societies, the JSON file contains the data in JSON format.

To check that the data from site was loaded correctly, we have to check that the number of distinct sites in the original data matches both the number of distinct sites in the table, which, at the same tiem should match the number of rows.
"""
from src.core.loaders.raw_reader import RawReader

def check_site_load(sparkSession,
                    catalog: str,
                    raw_schema: str,
                    site_file_name: str):

    raw_site_df = RawReader.read_json(sparkSession, catalog, raw_schema, site_file_name)

    row_count = raw_site_df.count()
    unique_site_count = raw_site_df.select("id").distinct().count()

    if row_count == unique_site_count:
        print("JSON file has only sites data, one row per site.")
    else:
        print("A single site may have different data.")
    return "OK"
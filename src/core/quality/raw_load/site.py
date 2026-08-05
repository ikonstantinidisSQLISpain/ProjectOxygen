
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
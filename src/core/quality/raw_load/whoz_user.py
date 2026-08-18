
"""
The checks that need to be done on whoz user data are the next.

* Each ID 

"""

from src.core.loaders.raw_reader import RawReader


def user_checks(sparkSession,
                catalog: str,
                raw_schema: str,
                user_file_name: str):

    raw_user_df = RawReader.read_json(sparkSession, catalog, raw_schema, user_file_name)

    row_count = raw_user_df.count()
    unique_user_count = raw_user_df.select("id").distinct().count()

    if row_count == unique_user_count:
        print("JSON file has only user data, one row per user.")
    else:
        print("A single user may have different data.")

    return "OK"


def user_checks2(sparkSession,
                catalog: str,
                raw_schema: str,
                user_file_name: str):
    """Checks if the created_by and last_modified_by are referencing another user."""

    raw_user_df = RawReader.read_json(sparkSession, catalog, raw_schema, user_file_name)

    unique_ids = raw_user_df.select("id").distinct()

    created_by = raw_user_df.select("created_by").distinct()
    last_modified_by = raw_user_df.select("last_modified_by").distinct()

    created_count = created_by.count()
    last_modified_count = last_modified_by.count()

    matching = created_by.join(unique_ids, how)

    return "Ok"
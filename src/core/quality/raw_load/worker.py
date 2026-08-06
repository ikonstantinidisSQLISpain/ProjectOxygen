from src.core.loaders.raw_reader import RawReader
from src.core.quality.utils import uniqueness_checking


def worker_checks(sparkSession,
                  catalog: str,
                  raw_schema: str,
                  worker_file_name: str):

    raw_worker_df = RawReader.read_json(sparkSession, catalog, raw_schema, worker_file_name)

    row_count = raw_worker_df.count()
    unique_worker_count = raw_worker_df.select("id").distinct().count()

    if row_count == unique_worker_count:
        print("JSON file has only worker data, one row per worker.")
    else:
        print("A single worker may have different data.")
    # The external IDs should be variant, we check if for each worker_id there is only one society id, same for entity

    if uniqueness_checking(raw_worker_df, "id", "department.id"):
        print("Each worker has only one department.")
    else:
        print("Some worker may have more than one department.")

    if uniqueness_checking(raw_worker_df, "id", "service_line.id"):
        print("Each worker has only one service_line.")
    else:
        print("Some worker may have more than one service_line.")

    return "OK"
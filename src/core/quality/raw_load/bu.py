"""
The BU json data is very similar to both societies and entities, it have the data from the original table in JSON format, additionally, the table contains two foreing keys referencing both the entities
and societies it belongs to. The original data has this two columns in JSON format.

To check if the data was loaded correctly, the number of rows should match the number of unique ids.

Additionally, we need to check if the relationship between the unit and the FK are 1 to many or many to many. To do so, it is necessary to check that each `id` has one `society_id`, and one `entity_id`.
"""

from src.core.loaders.raw_reader import RawReader
from src.core.quality.utils import uniqueness_checking


def bu_checks(sparkSession,
              catalog: str,
              raw_schema: str,
              bu_file_name: str):

    raw_bu_df = RawReader.read_json(sparkSession, catalog, raw_schema, bu_file_name)

    row_count = raw_bu_df.count()
    unique_bu_count = raw_bu_df.select("id").distinct().count()

    if row_count == unique_bu_count:
        print("JSON file has only BU data, one row per BU.")
    else:
        print("A single BU may have different data.")
    # The external IDs should be variant, we check if for each bu_id there is only one society id, same for entity

    if uniqueness_checking(raw_bu_df, "id", "society.id"):
        print("Each BU has only one society.")
    else:
        print("Some BU may have more than one society.")

    if uniqueness_checking(raw_bu_df, "id", "entity.id"):
        print("Each BU has only one entity.")
    else:
        print("Some BU may have more than one entity.")

    return "OK"


def bu_checks_2(sparkSession,
                catalog: str,
                raw_schema: str,
                bu_file_name: str):

    raw_bu_df = RawReader.read_json(sparkSession, catalog, raw_schema, bu_file_name)

    return "Ok"





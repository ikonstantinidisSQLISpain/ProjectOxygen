import pandas as pd
import json
from pathlib import Path
from typing import Any

import pyspark.pipelines as dp


def load_json(path: str | Path) -> dict[Any, Any] | list[dict[Any, Any]]:
    """
    Loads a JSON file and return its contents as a Python dictionary.

    Args:
        path: Path to the JSON file.

    Returns:
        A dictionary containing the parsed JSON.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file does not contain valid JSON.
        TypeError: If the top-level JSON object is not a dictionary or list.
    """
    with Path(path).open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) and not isinstance(data, list):
        raise TypeError(f"Expected a JSON object, got {type(data).__name__}")

    return data


"""
The mappings are of the shape key:value where the key is the actual value and value is the anonymized value
"""

def map_json_to_table(path):

    og = load_json(path)  # This is a bad practice and should be fixed to a spark version. It will work cause the mappings are small.

    data = {
        'name': list(),
        'anonymized': list()
    }

    for k,v in og.items():
        data['name'].append(k)
        data['anonymized'].append(v)

    df = pd.DataFrame(data)
    return spark.createDataFrame(df)





CATALOG = spark.conf.get("read.catalog")
TARGET_SCHEMA = spark.conf.get("bronze.schema")
READ_SCHEMA = spark.conf.get("raw.schema")


PATHS = [
    f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/mapping_bucu_names.json",
    f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/mapping_company_names.json",
    f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/mapping_department_names.json",
    f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/mapping_emails.json",
    f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/mapping_entity_names.json",
    f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/mapping_ids.json",
    f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/mapping_service_line_names.json",
    f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/mapping_site_names.json",
    f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/mapping_skill_names.json",
    f"/Volumes/{CATALOG}/{READ_SCHEMA}/source/mapping_society_names.json"
]

def get_table_name(path):
    file_name = path.split("/")[-1].split(".")[0]
    
    table_name = file_name.split("_")
    if table_name[-1] == "names":
        table_name = table_name[1:-1]
    else:
        table_name = table_name[1:]
    table_name_str = "_".join(table_name)
    return table_name_str

def pipe_builder(path):

    table_name = get_table_name(path)
    print(path, table_name)
    @dp.table(
        name=f"{CATALOG}.{TARGET_SCHEMA}.{table_name}",
        comment=(
            f"Raw mappings {table_name}"
        ),
        table_properties={
            "quality": "bronze",
        },
    )
    def f():
        return map_json_to_table(path)

    return None


for path in PATHS:
    pipe_builder(path)



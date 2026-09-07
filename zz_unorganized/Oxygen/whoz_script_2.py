import json
from pathlib import Path
import numpy as np
import datetime as dt

def load_json(path: str | Path) -> dict:
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
    path = Path(__file__).parent / path
    with Path(path).open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) and not isinstance(data, list):
        raise TypeError(f"Expected a JSON object, got {type(data).__name__}")

    return data


def write_json(path: str | Path, data: dict) -> None:
    """
    Writes a dictionary to a JSON file. Makes dir if it doesn't exist.

    Args:
        path: Path to the JSON file.
        data: The dictionary to write.
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8") as f:
        json.dump(data, f)
    return None


BASE = Path("./RawDataStructure")
enities = ["certification", "profile", "skill", "talent", "user"]

def parse_type(type_name):
    mapper = {
        "str": "STRING",
        "bool": "BOOLEAN",
        "float": "FLOAT",
        "int": "INTEGER"
    }
    return mapper[type_name]

def is_timestamp(fecha):
    try:
        dt.datetime.strptime(fecha, "%Y-%m-%dT%H:%M:%S.%f")
        return True
    except ValueError:
        return False


def build_schemas(entity):

    file_path =  BASE / f"whoz__{entity}_report_anonymized_structure.json"

    data = load_json(file_path)

    schema = dict()
    for k,v in data.items():
        if k.endswith("_list"):
            if k.removesuffix("_list") in data.keys():
                continue
            else:
                nk = k.removesuffix("_list")
        else:
            nk = k
        if not k.endswith("_list"):
            if isinstance(v, str):
                if k.endswith("Date"):
                    if is_timestamp(v):
                        schema[nk] = {
                            "type": "TIMESTAMP",
                            "format": "yyyy-MM-dd'T'HH:mm:ss.SSS"
                        }
                    else:
                        schema[nk] = {
                            "type": "DATE",
                            "format": "yyyy-MM-dd"
                        }
                else:
                    schema[nk] = {
                        "type": parse_type(v)
                    }
            elif isinstance(v, dict):
                schema[nk] = {
                    "type": "VARIANT"
                }
        else:
            schema[nk] = {
                "type": "VARIANT"
            }

    write_json(Path(__file__).parent / f"./RawDataSchema/schema_{entity}.json", schema)
    return None



for e in enities:
    build_schemas(e)
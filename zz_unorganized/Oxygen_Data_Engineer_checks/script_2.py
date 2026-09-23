import json
from pathlib import Path
import copy
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
    with Path(path).open("r", encoding="utf-8-sig") as f:
        data = json.load(f)

    if not isinstance(data, dict) and not isinstance(data, list):
        raise TypeError(f"Expected a JSON object, got {type(data).__name__}")

    return data


def get_subfolders(folder):
    return [p for p in Path(folder).iterdir() if p.is_dir()]

def get_current_folder():
    return Path(__file__).parent

def get_json_files(folder):
    return [p for p in Path(folder).iterdir() if p.is_file() and p.name.lower().endswith("_file_stats.json")]


def parse_type(type_name):
    mapper = {
        "str": "STRING",
        "bool": "BOOLEAN",
        "float": "FLOAT",
        "int": "INTEGER",
        "dict": "VARIANT"
    }
    return mapper[type_name]



def is_timestamp_key(date_key):
    if date_key in ["createdDate", 
                    "lastModifiedDate", 
                    "completionRateLastComputedDate", 
                    "lastConnectionDate", 
                    "lastInvitationDate", 
                    "removedDate", 
                    "availabilityConfirmationDate"]:
        return True
    return False


def get_data_types(data):
    result = dict()
    for key, value in data.get("data", {}).items():
        if key == "rows":
            continue
        # Date tiene prioridad
        if "Date" in key:
            if is_timestamp_key(key):
                result[key] = {
                    "type": "TIMESTAMP",
                    "format": "yyyy-MM-dd'T'HH:mm:ss.SSS"
                }
            else:
                result[key] = {
                    "type": "DATE",
                    "format": "yyyy-MM-dd"
                }
            continue
        
        data_types = value.get("data_types", [])


        for item in data_types:
            if item != "NoneType":
                detected_type = item
                break

        for item in data_types:
            if item in ["dict", "list"]:
                detected_type = "dict"
                break

        result[key] = {
            "type": parse_type(detected_type)
        }

    return result


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


def main():

    folder = get_current_folder() / "app_report"

    platforms = get_subfolders(folder)

    for platform_folder in platforms:
        files = get_json_files(platform_folder)
        for file in files:
            fn = file.name
            t = fn.replace("_file_stats", "")
            data = load_json(file)
            r = get_data_types(data)
            tf = get_current_folder() / "Schemas" / t
            write_json(tf, r)


    return None

main()
import json
from pathlib import Path


def load_json(path: str | Path) -> dict:
    """
    Load a JSON file and return its contents as a Python dictionary.

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



def get_dict_structure(d: dict) -> dict:
    """
    Recursively get the structure of a dictionary, replacing values with their types.

    Args:
        d: The input dictionary.

    Returns:
        A dictionary representing the structure of the input dictionary.
    """
    if not isinstance(d, dict) and not isinstance(d, list):
        raise TypeError(f"Expected a dictionary or list, got {type(d).__name__}")

    structure = dict()
    if isinstance(d, list):
        current_structure = dict()
        for item in d:
            if isinstance(item, dict):
                current_structure = recursive_update(current_structure, get_dict_structure(item))
        structure = recursive_update(structure, current_structure)
        return structure

    for key, value in d.items():
        if isinstance(value, dict):
            structure[key] = get_dict_structure(value)
        elif isinstance(value, list):
            structure[key + '_list'] = get_dict_structure(value)
        else:
            structure[key] = type(value).__name__
    return structure


def write_json(path: str | Path, data: dict) -> None:
    """
    Write a dictionary to a JSON file. Makes dir if it doesn't exist.

    Args:
        path: Path to the JSON file.
        data: The dictionary to write.
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=10)
    return None


data = load_json('./RawDataStructure/onetbp_worklogs_anonymized_structure.json')

print('-'*20)
print(data['worklogs_by_type'].keys())
print('-'*20)
print(data['worklogs'].keys())
print('-'*20)
print(set(data['worklogs'].keys()) == set(data['worklogs_by_type'].keys()))
print('-'*20)
print('-'*20)
print('-'*20)
print(data['worklogs']['01/2027'].keys())
print('-'*20)
print(data['worklogs_by_type']['01/2027'].keys())
print('-'*20)



def get_structures_raw_json(): # Just ignore the name of this function
    """
    Get the structure of all JSON files in the RawData folder and write them to RawDataStructure folder.
    """
    raw_file = "onetbp_worklogs_anonymized_structure.json"
    raw_path = f"./RawDataStructure/{raw_file}"
    raw_data = load_json(raw_path)
    write_json(f"./{raw_file}_structure.json", raw_data)
    return None

# get_structures_raw_json()


import json
from pathlib import Path

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
    with Path(path).open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) and not isinstance(data, list):
        raise TypeError(f"Expected a JSON object, got {type(data).__name__}")

    return data

def recursive_update(previous_dict: dict, new_dict: dict) -> dict:
    """
    Recursively updates a dictionary.
    
    If a key within the dict contains a dictionary and both have it, it will update the inner dict.
    """
    updated_dict = dict(previous_dict)
    for k,v in new_dict.items():
        if isinstance(v, dict) and k in updated_dict.keys():
            updated_dict[k] = recursive_update(updated_dict[k], v)
        else:
            updated_dict[k] = v

    return updated_dict

def get_dict_structure(d: dict) -> dict:
    """
    Recursively gets the structure of a dictionary, replacing values with their types.

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

def get_files_folder(path: str | Path, ext: str) -> list[Path]:
    """
    Gets a list of all files in a folder.

    Args:
        path: Path to the folder.
        ext: The file extension to filter by.
    """
    return [f for f in Path(path).iterdir() if f.is_file() and f.suffix == ext]

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



def get_fields(structure: dict):
    """Returns all the possible fields in the structure json"""
    return list(structure.keys())

def get_relations(structure: dict, entity: str):
    """Returns all the possible relations in the structure"""
    relations = list()
    for field, data_type in field.items():
        if isinstance(data_type, dict): # In this case it is a possible FK.
            field_split = field.split('_')
            if field_split[-1] == 'list':
                relationship_type = 'multi'
            else:
                relationship_type = 'single'
            relations.append(
                {
                    'from': entity,
                    'to': '_'.join(field_split[:-1]),
                    'type': relationship_type,
                    'key_name': field
                }
            )
    return relations


def write_relations(relations, archivo="salida.txt"):
    with open(archivo, "w", encoding="utf-8") as f:
        for relation in relations:
            f.write('*--*' * 15 + '\n')
            for k, v in relation.items():
                f.write(f'    {k}: {v}\n')
            f.write('*--*' * 15 + '\n\n')
    return None


def structure_to_mermaid(structure: dict, entity):
    """
    Reads the structure dictionary and returns a mermaid erDiagram
    """

    
            


    return mermaid_text
















def get_structures_raw_json():
    """
    Gets the structure of all JSON files in the RawData folder and write them to RawDataStructure folder.
    """
    raw_files = get_files_folder("./RawData", ".json")
    for raw_file in raw_files:
        raw_data = load_json(raw_file)
        structure = get_dict_structure(raw_data)
        
        write_json(f"./RawDataStructure/{raw_file.stem}_structure.json", structure)
    return None


def gen_realtions():
    raw_files = get_files_folder("./RawData", ".json")
    relations = list()
    for raw_file in raw_files:
        raw_data = load_json(raw_file)
        structure = get_dict_structure(raw_data)
        relations.extend(get_relations(structure)) 
    write_relations(relations)

if __name__ == "__main__":
    #get_structures_raw_json()
    gen_realtions()
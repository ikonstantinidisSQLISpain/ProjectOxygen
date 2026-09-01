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



def recursive_update(previous_dict: dict, new_dict: dict) -> dict:
    """
    Recursively updates a dictionary.
    
    If a key within the dict contains a dictionary and both have it, it will update the inner dict.
    """
    updated_dict = copy.deepcopy(previous_dict)
    for k,v in new_dict.items():
        if isinstance(v, dict) and k in updated_dict.keys():
            updated_dict[k] = recursive_update(updated_dict[k], v)
        else:
            updated_dict[k] = v

    return updated_dict


def value_metadata(val):

    metadata = {
        'type': type(val).__name__
    }

    return metadata




def process(base, d1):

    if len(base.keys()) == 0:
        updated = {
            'types': set(),
            'unique_values': set()
        }
    else:
        updated = copy.deepcopy(base)

    
    

    for k in ["type", "val", "structure", "inner_types", "inner_structure", "inner_values", "types", "unique_values"]:
        try:
            v = d1[k]
            if k == "type":
                updated["types"].add(v)
            elif k == "val":
                if not isinstance(v, (dict, list)):
                    updated["unique_values"].add(v)
            elif k in ["types", "unique_values"]:
                updated[k].update(v)
            elif k in ["structure", "inner_structure"]:
                updated[k] = v
            else:
                updated[k] = v
        except KeyError:
            pass

    return updated

def custom_update(d1, d2):


    updated = copy.deepcopy(d1)
    for k,v in d2.items():
        try:
            updated[k] = process(d1[k], v)
        except KeyError:
            updated[k] = process(dict(), v)

    return updated



def single_dict_process(data):

    new_data = dict()

    
    for i, (k,v) in enumerate(data.items()):
        val_type = type(v).__name__
        base = {
            'type': val_type,
            "val": v
        }
        if isinstance(v, dict):
            base["structure"] = single_dict_process(v)
            
        elif isinstance(v, list):
            base["inner_types"] = set()
            base["inner_structure"] = dict()
            base["inner_values"] = set()
            for i,el in enumerate(v):
                if isinstance(el, dict):
                    base["inner_structure"] = custom_update(base["inner_structure"], single_dict_process(el))
                else:
                    base["inner_types"].add(type(el).__name__)
                    base["inner_values"].add(el)

        new_data[k] = base

    return new_data

def multi_dict_process(list_of_d):
    final = dict()
    for i, d in enumerate(list_of_d):
        final = custom_update(final, single_dict_process(d))
    return final

def get_dict_structure(d: dict | list) -> dict:
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
        structure = current_structure
        return structure

    for key, value in d.items():
        if isinstance(value, dict):
            structure[key] = get_dict_structure(value)
        elif isinstance(value, list):
            structure[key + '_list'] = get_dict_structure(value)
        else:
            structure[key] = [type(value).__name__]
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


def make_path(schema, entity):
    current_folder = Path(__file__).resolve().parent
    path_ = current_folder / "app_report" / f"{schema}" / f"{entity}"
    return path_

def get_all_entity_files(schema, entity):
    return get_files_folder(make_path(schema, entity), '.json')

def get_all_entity_file_paths(schema, entity):
    current_folder = Path(__file__).resolve().parent
    return [current_folder / file for file in get_all_entity_files(schema, entity)]



def postprocess_structure(old):

    new = dict()
    
    for k,v in old.items():
        if isinstance(v, dict):
            new[k] = postprocess_structure(v)
        elif isinstance(v, set):
            if len(v) <= 5:
                new[k] = list(v)
            new[f"{k}_len"] = len(v)
        else:
            new[k] = v

    #print(old, new)

    return new

import numpy as np

def estadisticas(valores):
    q25, q50, q75 = np.quantile(valores, [0.25, 0.50, 0.75])

    stats = {
        "max": max(valores),
        "min": min(valores),
        "avg": np.mean(valores).item(),
        "q25": q25.item(),
        "q50": q50.item(),
        "q75": q75.item()
    }

    return stats


def estadisticas_completas(valores):
    non_zero = [x for x in valores if x != 0]

    stats = {
        "real": estadisticas(valores),
        "non_zero": estadisticas(non_zero)
    }

    return stats

def get_snapshot_date(file_path):
    file_name = file_path.name
    date_str = file_name[:9]
    date = dt.datetime.strptime(date_str, "%Y-%m-%d")

    return date

def get_deltas(l_dates):
    prev = l_dates[0]
    deltas = list()
    for i, d in enumerate(l_dates):
        if i==0:
            continue
        delta = d - prev
        deltas.append(delta.days)
        prev = d
    return deltas

def get_mid_date(prev, next_date):

    if prev is None:
        return next_date
    if next_date is None:
        return prev

    delta = get_snapshot_date(next_date) - get_snapshot_date(prev)
    days = delta.days

    new = prev + timedelta(days=int(days/2))

    return new

def get_structures_raw_json(schema, entity):
    """
    Gets the structure of all JSON files in the RawData folder and write them to RawDataStructure folder.
    """
    

    structure = dict()
    raw_files = get_all_entity_file_paths(schema, entity)
    current_folder = Path(__file__).resolve().parent

    n_files = len(raw_files)
    structure["n_files"] = n_files

    dates = list()

    date_error_counter = 0
    for i, raw_file in enumerate(raw_files):
        raw_data = load_json(raw_file)
        structure = copy.deepcopy(custom_update(structure, multi_dict_process(raw_data)))

        if i == 0:
            prev = None
            next_date = raw_files[i+1]
        elif i == n_files - 1:
            next_date = None
            prev = raw_files[i-1]
        else:
            prev = raw_files[i-1]
            next_date = raw_files[i+1]

        try:
            sn_date = get_snapshot_date(raw_file)
            dates.append(sn_date)
        except ValueError:
            date_error_counter += 1
            
        

    deltas = get_deltas(dates)

    structure["stats"] = estadisticas_completas(deltas)
    structure["error_date_counter"] = date_error_counter

    structure = postprocess_structure(structure)
    path = current_folder / "app_report" / f"{schema}" / f"{entity}_structure.json"
    #print(path)
    write_json(path, structure)
    return None


def get_date_freq(schema, entity):

    raw_files = get_all_entity_file_paths(schema, entity)
    dates = list()
    


    date_error_counter = 0
    for i, raw_file in enumerate(raw_files):
        try:
            sn_date = get_snapshot_date(raw_file)
            dates.append(sn_date)
        except ValueError:
            date_error_counter += 1

    deltas = get_deltas(dates)

    stats = estadisticas_completas(deltas)  


    return stats

def get_first_file_data(schema, entity):

    raw_files = get_all_entity_file_paths(schema, entity)

    for raw_file in raw_files:
        all_data = load_json(raw_file)
        if len(all_data) > 1:
            break

    if isinstance(all_data, dict):
        all_data = [v for v in all_data.values()]

    file_data = dict()
    n_dicts = len(all_data)
    file_data["rows"] = n_dicts

    for data in all_data:
        for k,v in data.items():

            try:
                file_data[k]
            except KeyError:
                file_data[k] = {
                    "data_types": set(),
                    "unique_vals": set(),
                    "null_count": 0,
                    "appear_count": 0,
                }

            file_data[k]["appear_count"] += 1
            file_data[k]["data_types"].add(type(v).__name__)
            if not isinstance(v, (list, set, dict)):
                file_data[k]["unique_vals"].add(v)
            if v is None:
                file_data[k]["null_count"] += 1

            if isinstance(v, str):
                if v.lower() in ["null", "none", "-", ""]:
                    file_data[k]["null_count"] += 1

    for k, stats in file_data.items():
        if k == "rows":
            continue
        file_data[k]["unique_percen"] = len(stats["unique_vals"])/file_data["rows"]
        file_data[k]["not_appear_count"] = n_dicts - stats["appear_count"]
        file_data[k]["null_percen"] = (stats["null_count"] + file_data[k]["not_appear_count"])/file_data["rows"]


    return file_data


def get_files_data(schema, entity):

    current_folder = Path(__file__).resolve().parent

    final = {
        "data": get_first_file_data(schema, entity),
        "files": get_date_freq(schema, entity)
    }

    path = current_folder / "app_report" / f"{schema}" / f"{entity}_file_stats.json"
    #print(path)
    write_json(path, postprocess_structure(final))

    return None



schemas_entities = {
    "analytic": {"bu", "department", "entity", "service_line", "site", "society"},
    "cra": {"bilan_cra_report", "worklog"},
    "perso": {"collab_status_report", "leave_report", "workers"},
    "project": {"ca_collab_report", "financial_report", "project_dataware_report", "projects_report"},
    "whoz": {
        "whoz__accreditation_report",
        "whoz__certification_report",
        "whoz__profile_report",
        "whoz__skill_report",
        "whoz__talent_report",
        "whoz__user_report"
    }
}

get_structures_raw_json("analytic", "bu")

for schema, entities in schemas_entities.items():
    for entity in entities:
        print(schema, entity)
        #get_structures_raw_json(schema, entity)
        get_files_data(schema, entity)
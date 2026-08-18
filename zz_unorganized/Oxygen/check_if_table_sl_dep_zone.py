"""
This file has the code to check if the the relation between department, service lines and zones are with a middle table (service line, department, zone) are just intermediate tables, (service line, zone), (service line, department) and (department, zone)
"""

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



def check_if_list_match(l1, l2):

    for v in l1:
        if v not in l2:
            return False

    for v in l2:
        if v not in l1:
            return False

    return True

def remove_empty_list(dict_v):
    new_dict = dict(dict_v) #makes a copy
    keys_to_pop = list() # Store to list cause iterating when popping gives error
    for k,v in new_dict.items():
        if len(v)==0:
            keys_to_pop.append(k)

    for k in keys_to_pop:
        new_dict.pop(k)
    return new_dict

def check_if_keys_match(dict1, dict2):
    d1_keys = [str(k) for k in dict1.keys()]
    d2_keys = [str(k) for k in dict2.keys()]
    if check_if_list_match(d1_keys, d2_keys):
        return True
    else:
        return False
    return True

def check_if_dict_match(dict1, dict2, extra_str_print):

    """
    The generated dict are based on other data, we need to remove keys that hold values that
    are empty list, since this provide no actual information for the purpose of this function
    """

    dict1 = remove_empty_list(dict1)
    dict2 = remove_empty_list(dict2)

    d1_keys = [str(k) for k in dict1.keys()]
    d2_keys = [str(k) for k in dict2.keys()]

    they_match = True
    if check_if_list_match(d1_keys, d2_keys):
        print('Match keys')
        pass
    else:
        print("Keys dont match")
        missing = [k for k in d2_keys if k not in d1_keys]
        matching = [k for k in d2_keys if k in d1_keys]
        print(missing)
        print(matching)
        they_match = False
    
    for k,v in dict1.items():
        v1 = [str(val) for val in v]
        v2 = [str(val) for val in dict2[k]]
        if check_if_list_match(v1, v2):
            continue
        else:
            miss = [val for val in v1 if v1 not in v2]
            match_val = [val for val in v1 if v1 in v2]
            print("Missing values for key:", k, ':', miss)
            print("Matching", match_val)
            they_match = False
        
    if they_match:
        print("Match", extra_str_print)
    else:
        print("Don't match", extra_str_print)
    return None

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

"""To check if the relation is as mentioned.

It is necessary to read the json from departments, gather each service line and for each service line obtain the departments that are associated with such service line and compare it with the departments that are associated to the service line in the service line file.

Additionally, we have to check if the combination between service_line, department and zone match in both files.
"""

def process_(og_file_data, objective_col):

    data = dict()
    for og in og_file_data:
        for tar in og[objective_col]:
            try:
                data[tar]
            except KeyError:
                data[tar] = list()
            data[tar].append(og["id"])

    return data



def check_sl_dep_zone():
    departments = load_json("./RawData/analytic_department_anonymized.json")
    service_lines = load_json("./RawData/analytic_service_line_anonymized.json")

    # key_values_readFile
    sl_dep_dep = process_(departments, "associated_service_line")
    zone_dep_dep = process_(departments, "associated_zone")

    dep_sl_sl = process_(service_lines, "associated_practice")
    zone_sl_sl = process_(service_lines, "associated_zone")



    sl_dep_sl = {sl["id"]:list(set(sl["associated_practice"])) for sl in service_lines}
    sl_zone_sl = {sl["id"]:list(set(sl["associated_zone"])) for sl in service_lines}

    dep_sl_dep = {dep["id"]:list(set(dep["associated_service_line"])) for dep in departments}
    dep_zone_dep = {dep["id"]:list(set(dep["associated_zone"])) for dep in departments}


    write_json("./check/sl_dep_dep.json", sl_dep_dep)
    write_json("./check/zone_dep_dep.json", zone_dep_dep)
    write_json("./check/dep_sl_sl.json", dep_sl_sl)
    write_json("./check/zone_sl_sl.json", zone_sl_sl)
    write_json("./check/sl_dep_sl.json", sl_dep_sl)
    write_json("./check/sl_zone_sl.json", sl_zone_sl)
    write_json("./check/dep_sl_dep.json", dep_sl_dep)
    write_json("./check/dep_zone_dep.json", dep_zone_dep)


    check_if_dict_match(sl_dep_dep, sl_dep_sl, "sl_dep_dep - sl_dep_sl")
    check_if_dict_match(dep_sl_sl, dep_sl_dep, "dep_sl_sl - dep_sl_dep")

    # Since both match, we consider joinig dep-sl-zone
    # We check if both files have the same zones.
    if check_if_keys_match(zone_dep_dep, zone_sl_sl):
        print("Both files have the same zones")
    else:
        print("Both files have different zones, mid table dep, sl, zone is discarded")

    # Lastly we check the combination dep-sl-zone are the same using the next file combinations
    # 1-
        # zone_sl_sl --> sl_dep_sl
    # 2-
        # zone_dep_dep --> dep_sl_dep

    table_1 = list()
    for zone, sls in zone_sl_sl.items():
        for sl in sls:
            deps = sl_dep_sl[sl]
            for dep in deps:
                table_1.append((zone, sl, dep))

    table_2 = list()
    for zone, deps in zone_dep_dep.items():
        for dep in deps:
            sls = dep_sl_dep[dep]
            for sl in sls:
                table_2.append((zone, sl, dep))

    if check_if_list_match(table_1, table_2):
        print("The combination match, we can assume the mid table is (dep, sl, zone)")
    else:
        print("The combination does NOT match, we assume the tables are (dep, zones), (sl, zones), (dep, sl)")

    return None

check_sl_dep_zone()













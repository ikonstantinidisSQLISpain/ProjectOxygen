
import json
from pathlib import Path
import copy


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


def write_json(path: str | Path, data: dict) -> None:
    """
    Writes a dictionary to a JSON file. Makes dir if it doesn't exist.

    Args:
        path: Path to the JSON file.
        data: The dictionary to write.
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    return None



def worklogs_checks():
    worklogs = load_json("./RawData/onetbp_worklogs_anonymized.json")

    number_list = list()
    number2_list = list()

    type_list = set()
    tbp_id_list = set()
    worklog_list = set()
    project_code_list = set()
    project_name_list = set()

    abscence_list = set()
    project_list = set()

    projects = dict()
    projects_names = dict()

    project_abscence_sum = set()
    worklog_sums_list = set()
    worklog_sums_checks = set()

    for worklog in worklogs:
        for date, data in worklog["worklogs"].items():
            for number, sub_data in data.items():
                number_list.append(number)
                type_data = worklog["worklogs_by_type"][date][number]
                abscence_list.add(type_data["absence"])
                project_list.add(type_data["project"])
                sum_val = (float(type_data["absence"])
                            + float(type_data["project"]))
                project_abscence_sum.add(sum_val)
                # Here it is assumed they match
                sum_worklog = 0
                if isinstance(sub_data, list):
                    for sub_data2 in sub_data:
                        type_list.add(sub_data2["type"])
                        tbp_id_list.add(sub_data2["tbp_id"])
                        worklog_list.add(sub_data2["worklog"])
                        project_code_list.add(sub_data2["project_code"])
                        project_name_list.add(sub_data2["project_name"])
                        if sub_data2["worklog"] is not None:
                            sum_worklog += float(sub_data2["worklog"])
                        # --
                        try:
                            projects[sub_data2["project_code"]]
                        except KeyError:
                            projects[sub_data2["project_code"]] = {
                                'name': set(),
                                'proj': set(),
                                'abs': set(),
                                'tbp_id': set(),
                                'worklog': set(),
                                'type': set()
                            }

                        projects[sub_data2["project_code"]]["name"].add(sub_data2["project_name"])
                        projects[sub_data2["project_code"]]["tbp_id"].add(sub_data2["tbp_id"])
                        projects[sub_data2["project_code"]]["worklog"].add(sub_data2["worklog"])
                        projects[sub_data2["project_code"]]["type"].add(sub_data2["type"])

                        projects[sub_data2["project_code"]]["proj"].add(type_data["project"])
                        projects[sub_data2["project_code"]]["abs"].add(type_data["absence"])
                        try:
                            projects_names[sub_data2["project_name"]]
                        except KeyError:
                            projects_names[sub_data2["project_name"]] = {
                                'code': set(),
                                'proj': set(),
                                'abs': set(),
                                'tbp_id': set(),
                                'worklog': set(),
                                'type': set()
                            }
                        projects_names[sub_data2["project_name"]]["code"].add(sub_data2["project_code"])
                        projects_names[sub_data2["project_name"]]["tbp_id"].add(sub_data2["tbp_id"])
                        projects_names[sub_data2["project_name"]]["worklog"].add(sub_data2["worklog"])
                        projects_names[sub_data2["project_name"]]["type"].add(sub_data2["type"])

                        projects_names[sub_data2["project_name"]]["proj"].add(type_data["project"])
                        projects_names[sub_data2["project_name"]]["abs"].add(type_data["absence"])
                else:
                    for number2, sub_data2 in sub_data.items():
                        number2_list.append(number2)
                        type_list.add(sub_data2["type"])
                        tbp_id_list.add(sub_data2["tbp_id"])
                        worklog_list.add(sub_data2["worklog"])
                        project_code_list.add(sub_data2["project_code"])
                        project_name_list.add(sub_data2["project_name"])

                        if sub_data2["worklog"] is not None:
                            sum_worklog += float(sub_data2["worklog"])
                        try:
                            projects[sub_data2["project_code"]]
                        except KeyError:
                            projects[sub_data2["project_code"]] = {
                                'name': set(),
                                'proj': set(),
                                'abs': set(),
                                'tbp_id': set(),
                                'worklog': set(),
                                'type': set()
                            }
                        projects[sub_data2["project_code"]]["name"].add(sub_data2["project_name"])
                        projects[sub_data2["project_code"]]["tbp_id"].add(sub_data2["tbp_id"])
                        projects[sub_data2["project_code"]]["worklog"].add(sub_data2["worklog"])
                        projects[sub_data2["project_code"]]["type"].add(sub_data2["type"])
                        
                        projects[sub_data2["project_code"]]["proj"].add(type_data["project"])
                        projects[sub_data2["project_code"]]["abs"].add(type_data["absence"])
                        try:
                            projects_names[sub_data2["project_name"]]
                        except KeyError:
                            projects_names[sub_data2["project_name"]] = {
                                'code': set(),
                                'proj': set(),
                                'abs': set(),
                                'tbp_id': set(),
                                'worklog': set(),
                                'type': set()
                            }
                        projects_names[sub_data2["project_name"]]["code"].add(sub_data2["project_code"])
                        projects_names[sub_data2["project_name"]]["tbp_id"].add(sub_data2["tbp_id"])
                        projects_names[sub_data2["project_name"]]["worklog"].add(sub_data2["worklog"])
                        projects_names[sub_data2["project_name"]]["type"].add(sub_data2["type"])

                        projects_names[sub_data2["project_name"]]["proj"].add(type_data["project"])
                        projects_names[sub_data2["project_name"]]["abs"].add(type_data["absence"])
                # ------
                worklog_sums_list.add(sum_worklog)
                worklog_sums_checks.add(sum_worklog == sum_val)


    

    unique_numbers = [int(n) for n in set(number_list)]
    unique_numbers2 = [int(n) for n in set(number2_list)]
    print(unique_numbers)
    print(max(unique_numbers)) # 31
    print(min(unique_numbers)) # 1

    print(unique_numbers2)
    print(max(unique_numbers2)) # 4047
    print(min(unique_numbers2)) # 1

    print("abscence")
    print(abscence_list)
    print(project_list)

    print("extra")
    print(len(type_list))
    print(type_list)
    print(len(tbp_id_list))
    print(len(worklog_list))
    print(worklog_list)
    print(len(project_code_list))
    print(len(project_name_list))


    print("*"*10, "projects", "*"*10)
    #print(projects)
    print("*"*10, "*"*10)

    projects_copy = copy.deepcopy(projects)
    copy_2 = copy.deepcopy(projects)
    subset = dict()
    for p, dat in projects.items():
        subset[p] = dict()
        for k,v in dat.items():
            projects_copy[p][k] = list(v)
            copy_2[p][k] = len(v)
            if k != 'name':
                continue
            if len(v) > 1:
                subset[p][k] = list(v)

    write_json('./worklogs_2/projects.json', projects_copy)
    write_json('./worklogs_2/projects_2.json', copy_2)
    write_json('./worklogs_2/projects_3.json', subset)



    projects_names_copy = copy.deepcopy(projects_names)
    copy_2_names = copy.deepcopy(projects_names)
    subset_names = dict()
    for p, dat in projects_names.items():
        subset_names[p] = dict()
        for k,v in dat.items():
            projects_names_copy[p][k] = list(v)
            copy_2_names[p][k] = len(v)
            if k != 'code':
                continue
            if len(v) > 1:
                subset_names[p][k] = list(v)

    write_json('./worklogs_2/projects_names.json', projects_names_copy)
    write_json('./worklogs_2/projects_names_2.json', copy_2_names)
    write_json('./worklogs_2/projects_names_3.json', subset_names)

    write_json('./worklogs_2/type_list.json', list(type_list))
    write_json('./worklogs_2/tbp_id_list.json', list(tbp_id_list))
    write_json('./worklogs_2/worklog_list.json', list(worklog_list))
    write_json('./worklogs_2/project_code_list.json', list(project_code_list))
    write_json('./worklogs_2/project_name_list.json', list(project_name_list))
    write_json('./worklogs_2/abscence_list.json', list(abscence_list))
    write_json('./worklogs_2/project_list.json', list(project_list))
    write_json('./worklogs_2/project_abscence_sum.json', list(project_abscence_sum))

    write_json('./worklogs_2/worklog_sums_list.json', list(worklog_sums_list))
    write_json('./worklogs_2/worklog_sums_checks.json', list(worklog_sums_checks))



    # This basically means that the number represents the day of the month and year mentioned in the date key

    # The number2 is completly random and we can't infer what does this represents.
    # In addition to the fact that some of the values are stored in list an dont 
    # have a number_2 key

    """
    It is a strange behaviour that a single "project_code" has multiple different names, so we have to analyze it since a quick check, show that those projects with strange code are the only one that may have multiple names.

    subset was added for that purpose
    """

    return None

worklogs_checks()
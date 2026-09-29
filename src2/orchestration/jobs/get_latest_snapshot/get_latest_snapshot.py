
volumen_path = dbutils.widgets.get("latest_snapshots_file")


import json
from pathlib import Path
import datetime as dt


def get_folder_contents(folder: str) -> tuple[list[Path], list[Path]]:
    """
    Returns the directories and files contained in a folder.

    Args:
        folder: Path to the folder as a string.

    Returns:
        A tuple containing:
            - A list of directories.
            - A list of files.

    Raises:
        TypeError: If folder is not a string.
        FileNotFoundError: If the folder does not exist.
        NotADirectoryError: If the path is not a directory.
    """
    if not isinstance(folder, str):
        raise TypeError(
            f"folder must be a str, got {type(folder).__name__}"
        )

    folder_path = Path(folder)

    if not folder_path.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder_path}")

    if not folder_path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {folder_path}")

    directories = [path for path in folder_path.iterdir() if path.is_dir()]
    files = [path for path in folder_path.iterdir() if path.is_file()]

    return directories, files


def get_date_and_error(file_name: str):
    """
    Extracts the snapshot date if possible and an error description if it is not.

    Returns a tuple, first value is date or None, second is a string or None.
    """

    if not isinstance(file_name, str):
        return (None, "File name is not a string.")

    # Files are expected to have snapshot in the next format YYYY-MM-DD_file_name
    # We extract the first 10 character that are suppose to have a date.

    if len(file_name) < 11:
        return (None, "Date not provided.")

    date_str = file_name[:12]

    validity_count = 0
    n_chars = 11
    for i in range(n_chars + 1):
        char = date_str[i]
        if i in [4, 7]:
            if char != "-":
                continue
        elif i == 11:
            if char != "_":
                continue
        else:
            try:
                int(char)
            except ValueError:
                continue
        validity_count += 1

    if validity_count == n_chars:
        return date_str[:10]
    elif validity_count < 12 and validity_count > 8:
        return None
    else:
        return None
    return None

def str_to_date(date_str: str) -> dt.date:
    if not isinstance(date_str, str):
        raise TypeError(
            f"date_str must be a str, got {type(date_str).__name__}"
        )

    return datetime.strptime(date_str, "%d/%m/%Y").date()


def get_latest_snapshot_dates(folder: str) -> None:
    _, files = get_folder_contents(folder)

    dates_list = list()
    for file_path in files:
        date_str = get_date_and_error(file_path.name)
        dates_list.append(str_to_date(date_str))

    return max(dates_list)

def update_dict(
    data: dict,
    first_key: str,
    second_key: str,
    value
    ) -> None:
    if first_key not in data:
        data[first_key] = {}

    data[first_key][second_key] = value

    return None


def read_json(file_path: str) -> dict:
    if not isinstance(file_path, str):
        raise TypeError(
            f"file_path must be a str, got {type(file_path).__name__}"
        )

    path = Path(file_path)

    # Crear el archivo si no existe
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", encoding="utf-8") as file:
            json.dump({}, file, indent=4)

        return {}

    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")

    # Si el archivo está vacío, devolver {}
    if path.stat().st_size == 0:
        return {}

    with path.open("r", encoding="utf-8") as file:
        content = file.read().strip()

    # Si solo contiene espacios/saltos de línea, devolver {}
    if not content:
        return {}

    return json.loads(content)


def write_json(data: dict, file_path: str) -> None:
    if not isinstance(data, dict):
        raise TypeError(
            f"data must be a dict, got {type(data).__name__}"
        )

    if not isinstance(file_path, str):
        raise TypeError(
            f"file_path must be a str, got {type(file_path).__name__}"
        )

    path = Path(file_path)

    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


def get_table_volume(spark, vol_type, platform, table_name):
    # Searchs the config for a volume associated to that platform and table, if it does not exist it returns the platform
    # specific, and if it does fail aswell it returns the default volume.
    
    parts = ["volume", vol_type, platform, table_name]

    for k in range(len(parts), 1, -1):
        vol = get_conf(spark, ".".join(parts[:k]))
        if vol is not None:
            return vol

    raise ValueError(f"Volume path not found in config. {vol_type}.{platform}.{table_name}")
    return None


def filter_files(folder: str, pattern: str) -> list[Path]:
    if not isinstance(folder, str):
        raise TypeError(
            f"folder must be a str, got {type(folder).__name__}"
        )

    if not isinstance(pattern, str):
        raise TypeError(
            f"pattern must be a str, got {type(pattern).__name__}"
        )

    _, files = get_folder_contents(folder)

    return [
        file_path
        for file_path in files
        if file_path.match(pattern)
    ]


def get_table_glob_regex(spark, platform, table_name):
    return get_conf(spark, f"glob.data.{platform}.{table_name}")


PLATFORM_TABLES = {
    "analytic": ["bu", "department", "entity", "service_line", "site", "society"],
    "whoz": ["accreditation", "certification", "profile", "skill", "talent", "user"],
    "perso": ["collab_status", "leave", "workers"]
}

for platform, tables in PLATFORM_TABLES.items():
    for t in tables:
        ls_data = read_json(volumen_path)
        ls_date_pre = ls_data.get(platform, dt.datetime(1000, 1, 1)).get(t, dt.datetime(1000, 1, 1))
        ls_date_new = filter_files(get_table_volume(spark, "data", platform, t), get_table_glob_regex(spark, platform, t))
        update_dict(ls_data, platform, t, ls_date_new)

write_json(ls_data, volumen_path)

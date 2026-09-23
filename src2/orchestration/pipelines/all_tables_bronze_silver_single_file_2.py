# constants.py



METADATA_COLUMNS = [
    "_source_file",
    "_source_file_name",
    "_source_file_size",
    "_source_file_modified_at"
]

SNAPSHOT_COL = "snapshot_ts"
INGEST_TS_COL = "ingest_ts"


# utils.py

import sys, os
"""
for _p in [os.path.abspath(p) for p in ['', '..']]:
    if os.path.isdir(os.path.join(_p, 'common')) and _p not in sys.path:
        sys.path.insert(0, _p)
        break
"""
for _p in [os.path.abspath(p) for p in ['', '..']]:
    if os.path.isdir(os.path.join(_p, 'common')) and _p not in sys.path:
        sys.path.insert(0, _p)
        break


import json
from pathlib import Path
import pyspark.sql.functions as F
import pyspark.sql.types as ty
# import common.constants as c
from functools import reduce
import datetime as dt
import pandas as pd



def get_conf(spark, config_key):
    return spark.conf.get(config_key, None)

def get_quality(spark, quality):
    return get_conf(spark, f"qualities.{quality}")

def get_platform(spark, platform):
    return get_conf(spark, f"platforms.{platform}")

def get_quarantine_schema(spark):
    return get_conf(spark, "quarantine.schema")

def get_quarantine_table(spark, quarantine_table):
    return get_conf(spark, f"quarantine.{quarantine_table}")

def schema_name_builder(quality, platform):
    return f"{quality}_{platform}"

def table_name_builder(tag, name):
    return f"{tag}_{name}"


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

def get_table_glob_regex(spark, platform, table_name):
    return get_conf(spark, f"glob.data.{platform}.{table_name}")

def get_table_metadata_files(spark, file_type, platform, table_name):
    parts = ["file", file_type, platform, table_name]
    for k in range(len(parts), 1, -1):
        file = get_conf(spark, ".".join(parts[:k]))
        if file is not None:
            return file

    raise ValueError(f"File path not found in config. {file_type}.{platform}.{table_name}")
    return None


def parse_date(fecha_str, valid_formats):

    for formato in valid_formats:
        try:
            return dt.datetime.strptime(fecha_str, formato)
        except ValueError:
            pass

    raise ValueError(f"Invalid date format: {fecha_str} [{valid_formats}]")


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



def all_in(lista1, lista2):
    return all(element in list(lista2) for element in list(lista1))

def any_in(l1, l2):
    for e in l1:
        if e in l2:
            return True
    return False

def get_table_config(config_file_path, platform, table, default_file_path):
    platform_data = load_json(config_file_path).get(platform, None)
    if platform_data is None:
        return load_json(default_file_path)
    table_data = platform_data.get(table, None)
    if table_data is None:
        return load_json(default_file_path)
    return table_data




def middle_table_extractor(sparkSession, 
                           df, 
                           df_pks, 
                           col_to_extract, 
                           pk_names, 
                           final_cols_names, 
                           sub_paths = None):

    """
Extracts values from a Variant column containing a list and generates a
flattened intermediate (middle) table.

This function is intended to transform columns containing arrays stored
as Variant values into a normalized dataframe suitable for representing
many-to-many relationships.

Supported input structures
--------------------------

1. Array of strings:

[
    "id1",
    "id2",
    "id3"
]

Produces:

+------------+---------+
| source_pk  | target  |
+------------+---------+
| ...        | id1     |
| ...        | id2     |
| ...        | id3     |
+------------+---------+

2. Array of objects:

[
    {"id": "id1", "name": "User 1"},
    {"id": "id2", "name": "User 2"}
]

In this case, `sub_paths` must be provided to indicate which field(s)
should be extracted from each object.

The function preserves the specified primary key columns from the source
dataframe and explodes the array values into individual rows.

Parameters
----------
sparkSession : SparkSession
    Active Spark session.

df : pyspark.sql.DataFrame
    Source dataframe containing the Variant column to process.

df_pks : str | list[str]
    Column(s) acting as primary keys in the source dataframe.

col_to_extract : str
    Variant column containing the array to be exploded.

pk_names : str | list[str]
    Output names for the primary key columns in the resulting dataframe.
    Length must match `df_pks`.

final_cols_names : str | list[str]
    Name(s) of the output column(s) containing the extracted values.

    - If `sub_paths` is None, a single string is expected.
    - If `sub_paths` is provided, one output name must be supplied for
      each path in `sub_paths`.

sub_paths : str | list[str] | None, default=None
    JSON path(s) to extract from each array element when the elements
    are JSON objects.

    Examples:
        "id"
        "$.id"
        "$.user.id"

    If None, array elements are assumed to be plain string values and
    are returned directly.

Returns
-------
pyspark.sql.DataFrame
    A dataframe containing:

    - The primary key columns renamed according to `pk_names`.
    - One row per element of the exploded list.
    - The extracted value column(s).

Raises
------
TypeError
    If any parameter is provided with an invalid type.

ValueError
    If:

    - Primary key columns do not exist in the dataframe.
    - The column to extract does not exist in the dataframe.
    - The number of PK columns does not match the number of PK names.
    - The number of output column names does not match the number of
      provided sub-paths.
    - Required mappings are missing or inconsistent.

Notes
-----
- Variant arrays are converted using:

      parse_json(cast(column as string))

- Array expansion is delegated to:

      explode_variant_list_column()

- When `sub_paths` is provided, values are extracted using:

      variant_get()

Examples
--------
Extract an array of string IDs:

>>> middle_table_extractor(
...     spark,
...     df,
...     df_pks="project_id",
...     col_to_extract="user_ids",
...     pk_names="project_id",
...     final_cols_names="user_id"
... )

Extract a single field from an array of objects:

>>> middle_table_extractor(
...     spark,
...     df,
...     df_pks="project_id",
...     col_to_extract="users",
...     pk_names="project_id",
...     final_cols_names="user_id",
...     sub_paths="id"
... )

Extract multiple fields from an array of objects:

>>> middle_table_extractor(
...     spark,
...     df,
...     df_pks="project_id",
...     col_to_extract="users",
...     pk_names="project_id",
...     final_cols_names=["user_id", "user_name"],
...     sub_paths=["id", "name"]
... )
"""

    cols = df.columns

    
    if not isinstance(df_pks, (str, list)):
        raise TypeError("df_pks must be a list or str")

    if isinstance(df_pks, str):
        df_pks = [df_pks]

    if not isinstance(pk_names, (str, list)):
        raise TypeError("pk_names must be a list or str")

    if isinstance(pk_names, str):
        pk_names = [pk_names]

    if not isinstance(col_to_extract, str):
        raise TypeError("col_to_extract must be a str")



    if not isinstance(final_cols_names, (str, list)):
        raise TypeError("final_cols_names must be a list or str")

    if isinstance(final_cols_names, str):
        final_cols_names = [final_cols_names]

    if sub_paths is None:
        if not isinstance(final_cols_names, str):
            raise ValueError("final cols names must be a string if sub_paths is not provided.")

    if len(df_pks) != len(pk_names):
        raise ValueError("PK cols len must match names len")


    if not all_in(df_pks, cols):
        raise ValueError(f"provided Pks {df_pks, cols} are not in dataframe.")
    
    if col_to_extract not in cols:
        raise ValueError(f"provided col_to_extract {col_to_extract, cols} are not in dataframe.")


    cols_pk = [F.col(col).alias(name) for col, name in zip(df_pks, pk_names)]
    cols_name = [F.col(name) for col, name in zip(df_pks, pk_names)]
    cols_ini = list(df_pks)
    cols_ini.append(col_to_extract)
    ndf = df.select(*cols_ini)

    ndf = ndf.withColumn(
        col_to_extract,
        F.expr(f"parse_json(cast({col_to_extract} as string))")
    )
    ndf = explode_variant_list_column(sparkSession, ndf, col_to_extract)
    ndf = ndf.select(*cols_pk, F.col("value").alias(col_to_extract))

    

    if sub_paths is not None:
        if not isinstance(sub_paths, (str, list)):
            raise TypeError("sub_paths must be a list or str")

        if isinstance(sub_paths, str):
            sub_paths = [sub_paths]
        if len(final_cols_names) != len(sub_paths):
            raise ValueError("final_cols_names len must match sub_paths len")
        
        ndf = ndf.select(
            *cols_name, 
            *[
                F.expr(
                    f"""variant_get({col_to_extract}, '$.{sub_path.replace("$.", "")}', 'string')"""
                ).alias(final_col_name)
                for final_col_name, sub_path in zip(final_cols_names, sub_paths)
            ]
        )


    return ndf


def explode_variant_list_column(sparkSession, df, col_to_explode):
    """Returns the same df but with the col_to_explode column exploded"""
    if col_to_explode not in df.columns:
        raise ValueError(f"{col_to_explode} not in df columns: {df.columns}")
    ndf = df.lateralJoin(
        sparkSession.tvf.variant_explode(F.col(col_to_explode).outer()) # Outer is needed if you want to use this expression in a pipeline
    )
    return ndf






def remove_cols(df, cols_to_drop, remove_metadata=True):

    if not isinstance(cols_to_drop, (str, list)):
        raise TypeError("cols to drop must be a list or str")

    if isinstance(cols_to_drop, str):
        cols_to_drop = [cols_to_drop]

    cols = df.columns

    if not all_in(cols_to_drop, cols):
        raise ValueError(f"cols to drop are not in columns: {cols_to_drop} {cols}")

    cols_to_remove = list(cols_to_drop)
    if remove_metadata:
        cols_to_remove.extend(METADATA_COLUMNS)
        cols_to_remove.append(SNAPSHOT_COL)
        cols_to_remove.append(INGEST_TS_COL)
        cols_to_remove.append("remaining_payload")

    
    ndf = df.drop(*cols_to_remove)

    return ndf


def filter_conditions(df, list_of_constraints):
    "Returns the filtered df with the rows that match all the conditions"
    "list of contrains must be a list of contraint sql queries"
    if not isinstance(list_of_constraints, list):
        raise TypeError("List of constraints is not a list")

    sql_statement = ""
    n_cons = len(list_of_constraints)
    for i, cons in enumerate(list_of_constraints):
        operator = " AND "
        if i==n_cons-1:
            operator = ""

        sql_statement = sql_statement + cons + operator

    ndf = df.where(sql_statement)

    return ndf


def make_quarantine_table(df, dict_of_constraints, pk_cols, table_path):
    "Returns the filtered df with the rows PK that miss any of the conditions"
    "list of contrains must be a list of tuple (constraint_query, error_description)"
    if not isinstance(dict_of_constraints, dict):
        raise TypeError("List of constraints is not a list")

    if not isinstance(pk_cols, (str, list)):
        raise TypeError("PK cols is not a list or str")

    if isinstance(pk_cols, str):
        pk_cols = [pk_cols]

    list_of_df = list()
    for cons, desc in dict_of_constraints.items():

        sub_df = df.where(cons)
        sub_df = sub_df.withColumn("error_description", F.lit(desc))
        sub_df = sub_df.select(*pk_cols, "error_description")
        list_of_df.append(sub_df)

    
    df_final = reduce(lambda df1, df2: df1.union(df2), list_of_df)


    df_agg = (
        df_final.groupBy(*pk_cols)
        .agg(
            F.concat_ws(" | ", F.collect_list("error_description")).alias("errors"),
        )
    )

    df_agg = df_agg.withColumn("table_path", F.lit(table_path))
    df_agg = df_agg.withColumn("pk_vals", 
                                F.concat_ws(
                                    " | ",
                                    *[
                                        F.concat(
                                            F.lit(f"{col}: "),
                                            F.coalesce(F.col(col).cast("string"), F.lit(""))
                                        )
                                        for col in pk_cols
                                    ]
                                )
                                )
    qdf = df_agg.drop(*pk_cols)
    return qdf



def extract_variant_keys(df, variant_col, variant_keys_formats):
    if not isinstance(variant_keys_formats, dict):
        raise ValueError("Variant keys format is not a dict")

    ndf = df

    for key, format_type in variant_keys_formats.items():
        ndf = ndf.withColumn(
            f"{variant_col}_{key}",
            F.expr(f"variant_get({variant_col}, '$.{key}', '{format_type}')")
        )
    return ndf



def classify_status_code(
    spark,
    df,
    status_code_column: str):
    csv_path = spark.conf.get("file.encode.collab_status")

    status_mapping = (
        pd.read_csv(csv_path)
        .set_index("status_code")["status_name"]
        .to_dict()
    )

    mapping_expr = F.create_map(
        *[F.lit(x) for kv in status_mapping.items() for x in kv]
    )

    df = df.withColumn(
        f"{status_code_column}_name",
        mapping_expr[F.col("status")]
    )

    return df





def add_bucket_and_status(df, column: str):
    return (
        df
        .withColumn(
            f"{column}_Bucket",
            F.when(F.col(column) < 0.2, "0-0.2")
             .when(F.col(column) < 0.4, "0.2-0.4")
             .when(F.col(column) < 0.6, "0.4-0.6")
             .when(F.col(column) < 0.8, "0.6-0.8")
             .otherwise("0.8-1")
        )
        .withColumn(
            f"{column}_Status",
            F.when(F.col(column) > 0.8, "Completed")
             .otherwise("Not completed")
        )
    )


def add_date_status(df, column: str):
    days_since = F.datediff(F.current_date(), F.to_date(F.col(column)))

    return (
        df
        .withColumn(
            "DaysSince",
            days_since
        )
        .withColumn(
            f"{column}_Status",
            F.when(F.col(column).isNull(), None)
             .when(days_since < 7, "Active")
             .when(days_since < 30, "Inactive - 30 days")
             .when(days_since < 90, "Inactive - 90 days")
             .otherwise("Inactive 90+ days")
        )
    )


def add_seniority(df, column: str):
    return (
        df
        .withColumn(
            "Seniority",
            F.when(F.col(column).isNull(), None)
             .when(F.col(column) < 2, "Junior")
             .when(F.col(column) < 5, "Intermediate")
             .when(F.col(column) < 10, "Experienced")
             .otherwise("Senior")
        )
    )


def simple_word_extractor(text):
    return text.strip().split(" ")

def simple_word_counter(text):
    return len(simple_word_extractor(text))


def calculate_profile_completion_score(base_like_profile_df):

    return ndf


class ProfileScoreNamespace():
    """Class that holds all the functions that are needed to calculate the completion_score"""
    def __init__(self):

        self.conditions_map = {
            "c1": {
                "function": self.positions_have_assignment,
                "name": "positions_have_assignment",
                "columns": ["positions"], # Table column names required for this function
                "weight": 15
            },
            "c2": {
                "function": self.positions_have_skills,
                "name": "positions_have_skills",
                "columns": ["positions"],
                "weight": 15
            },
            "c3": {
                "function": self.assignments_have_skills,
                "name": "assignments_have_skills",
                "columns": ["positions"],
                "weight": 15
            },
            "c4": {
                "function": self.short_project_context,
                "name": "short_project_context",
                "columns": ["positions"],
                "weight": 15
            },
            "c5": {
                "function": self.favourite_skills,
                "name": "favourite_skills",
                "columns": ["aptitudes"],
                "weight": 15
            },
            "c6": {
                "function": self.profile_updated,
                "name": "profile_updated",
                "columns": ["lastModifiedDate", SNAPSHOT_COL],
                "weight": 15
            },
            "c7": {
                "function": self.position_fields_filled,
                "name": "position_fields_filled",
                "columns": ["positions"],
                "weight": 3
            },
            "c8": {
                "function": self.assignment_fields_filled,
                "name": "assignment_fields_filled",
                "columns": ["positions"],
                "weight": 3
            },
            "c9": {
                "function": self.no_forbidden_words,
                "name": "no_forbidden_words",
                "columns": ["positions"],
                "weight": 3
            },
            "c10": {
                "function": self.has_activity_area,
                "name": "has_activity_area",
                "columns": ["aptitudes"],
                "weight": 8
            },
            "c11": {
                "function": self.no_free_text_skills,
                "name": "no_free_text_skills",
                "columns": ["aptitudes"],
                "weight": 8
            },
            "c12": {
                "function": self.proficiency_on_twenty_pct,
                "name": "proficiency_on_twenty_pct",
                "columns": ["aptitudes"],
                "weight": 8
            },
            "c13": {
                "function": self.all_skills_categorized,
                "name": "all_skills_categorized",
                "columns": ["aptitudes"],
                "weight": 3
            },
            "c14": {
                "function": self.education_field_filled,
                "name": "education_field_filled",
                "columns": ["educations"],
                "weight": 3
            },
            "c15": {
                "function": self.bio_word_count,
                "name": "bio_word_count",
                "columns": ["headline"],
                "weight": 3
            },
            "c16": {
                "function": self.skills_linked_to_positions,
                "name": "skills_linked_to_positions",
                "columns": ["aptitudes","positions"],
                "weight": 8
            },
        }

        self.weight_sum = sum([data["weight"] for data in self.conditions_map.values()])

        return None

    @staticmethod
    def positions_have_assignment(profile_positions_col):

        if profile_positions_col is None:
            return False
        
        """Takes a profile base (see databricks instance) row and calculates if the condition is met."""
        """
        The process for this condition is the next:

        1. Gather the positions.
        2. For each position check if it has a parent
        """
        positions = profile_positions_col.toPython()


        position_missions = dict()
        n_positions = len(positions)

        if n_positions == 0:
            return False
        
        for p in positions:
            parent_id = p.get("parentPositionId", None)
            if parent_id is not None:
                pos_mis = position_missions.get(parent_id, None)
                if pos_mis is None:
                    position_missions[parent_id] = {
                        "mission_counter": 0,
                        "non_mission_counter": 0, # Non missions should be the parent, and should always be 0
                        "child_positions_counter": 0
                    }

                position_missions[parent_id]["child_positions_counter"] += 1
                if p.get("isMission", None):
                    position_missions[parent_id]["mission_counter"] += 1
                else:
                    position_missions[parent_id]["non_mission_counter"] += 1

        pos_with_missions = 0
        for parent, data in position_missions.items():
            if data["mission_counter"] >= 1:
                pos_with_missions += 1

            if data["non_mission_counter"] > 0:
                raise ValueError("non_mission_counter > 0, ERROR IN CODE")

        if pos_with_missions == len(position_missions.keys()):
            return True

        return False

    @staticmethod
    def positions_have_skills(profile_positions_col):

        if profile_positions_col is None:
            return False

        """Checks if all the non mission positions have skills associated."""

        positions = profile_positions_col.toPython()
        n_positions = len(positions)

        if n_positions == 0:
            return False

        position_skill = dict()
        for p in positions:
            mission = p.get("isMission", None)
            if not mission or mission is None:
                if position_skill.get(p["id"], None) is None:
                    position_skill[p["id"]] = False
                aptitudes = p.get("aptitudeReferences", None)
                if aptitudes is not None:
                    if len(aptitudes) > 0:
                        position_skill[p["id"]] = True

        if all(position_skill.values()):
            return True
        return False

    @staticmethod
    def assignments_have_skills(profile_positions_col):

        if profile_positions_col is None:
            return False
        
        """Same as positions_have_skills, but with missions"""
        positions = profile_positions_col.toPython()
        n_positions = len(positions)

        if n_positions == 0:
            return False
        
        position_skill = dict()
        for p in positions:
            mission = p.get("isMission", None)
            if mission:
                if position_skill.get(p["id"], None) is None:
                    position_skill[p["id"]] = False
                aptitudes = p.get("aptitudeReferences", None)
                if aptitudes is not None:
                    if len(aptitudes) > 0:
                        position_skill[p["id"]] = True

        if all(position_skill.values()):
            return True
        return False

    @staticmethod
    def short_project_context(profile_positions_col):

        if profile_positions_col is None:
            return False
        
        """Checks if missionContext or Description wordCount is in valid range(30, 150) both included"""
        positions = profile_positions_col.toPython()
        n_positions = len(positions)

        if n_positions == 0:
            return False

        word_count_in_range = lambda count: count >= 30 and count <= 150
        n_positions = len(positions)
        correct_counter = 0
        for p in positions:
            if p.get("isMission", None):
                desc = p.get("description", None)
                if desc is not None:
                    w_count = simple_word_counter(desc)
                    if word_count_in_range(w_count):
                        correct_counter += 1
            elif p.get("isMission", None) is None:
                pass
            else:
                m_context = p.get("missionContext", None)
                if m_context is not None:
                    w_count = simple_word_counter(m_context)
                    if word_count_in_range(w_count):
                        correct_counter += 1

        if correct_counter == n_positions:
            return True
        return False

    @staticmethod
    def favourite_skills(aptitudes_column):

        if aptitudes_column is None:
            return False
        
        """Checks if favourite skills is in range(5, 8) both included"""
        range_check = lambda count: count >= 5 and count <= 8

        aptitudes = aptitudes_column.toPython()
        n_aptitudes = len(aptitudes)

        if n_aptitudes == 0:
            return False

        fav_counter = 0
        for ap in aptitudes:
            if ap.get("visibility", None) == "FAVOURITE":
                fav_counter += 1

        if range_check(fav_counter):
            return True

        return False

    @staticmethod
    def profile_updated(last_modified_date_column, snapshot_ts_column):

        if last_modified_date_column is None:
            return False
        
        """Returns True if the profile was updated within a year of the snapshot."""
        lm = parse_date(str(last_modified_date_column), ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"])
        sn = dt.datetime.strptime(str(snapshot_ts_column), "%Y-%m-%d")

        delta = sn.date() - lm.date()
        delta_days = delta.days

        if delta_days <= 365:
            return True
        return False

    @staticmethod
    def position_fields_filled(profile_positions_col):

        if profile_positions_col is None:
            return False
        
        """Checks if all the positions have the mandatory fields filled."""
        positions = profile_positions_col.toPython()

        n_positions = len(positions)
        if n_positions == 0:
            return False

        fields_filled_counter = 0
        pos_counter = 0
        mandatory_fields = ["title", "employerName", "address.city", "startDate", "description"]
        for p in positions:
            if not p.get("isMission", None):
                continue
            pos_counter += 1
            conditions = list()
            for f in mandatory_fields:
                if p.get(f, None) is not None:
                    if f == "address.city":
                        if p.get(f).get("city", None) is not None:
                            conditions.append(True)
                    else:
                        conditions.append(True)
            if all(conditions):
                fields_filled_counter += 1

        if fields_filled_counter == pos_counter:
            return True
        
        return False

    @staticmethod
    def assignment_fields_filled(profile_positions_col):

        if profile_positions_col is None:
            return False
        
        """Checks if all the assignments have the mandatory fields filled."""
        positions = profile_positions_col.toPython()

        n_positions = len(positions)
        if n_positions == 0:
            return False

        fields_filled_counter = 0
        pos_counter = 0
        mandatory_fields = ["title", "employerName", "address.city", "startDate", "description"]
        for p in positions:
            if p.get("isMission", None):
                continue
            pos_counter += 1
            conditions = list()
            for f in mandatory_fields:
                if p.get(f, None) is not None:
                    if f == "address.city":
                        if p.get(f).get("city", None) is not None:
                            conditions.append(True)
                    else:
                        conditions.append(True)
            if all(conditions):
                fields_filled_counter += 1

        if fields_filled_counter == pos_counter:
            return True

        return False

    @staticmethod
    def no_forbidden_words(profile_positions_col):

        if profile_positions_col is None:
            return False
        

        """Checks for forbidden words in the description"""
        forbidden_words = ["context", "methods", "tools", "missions"]
        positions = profile_positions_col.toPython()

        n_positions = len(positions)
        if n_positions == 0:
            return False

        forbidden_words_counter = 0
        for p in positions:
            if p.get("isMission", None) is None:
                continue
            elif p.get("isMission", None):
                context = p.get("missionContext", None)
                if context:
                    words = [w.lower() for w in simple_word_extractor(context)]
                    if any_in(words, forbidden_words):
                        forbidden_words_counter += 1
                else:
                    forbidden_words_counter += 1
            else:
                desc = p.get("description", None)
                if desc:
                    words = simple_word_extractor(desc)
                    if any_in(words, forbidden_words):
                        forbidden_words_counter += 1
                else:
                    forbidden_words_counter += 1
        return False

    @staticmethod
    def has_activity_area(aptitudes_column):

        if aptitudes_column is None:
            return False
        
        """Checks if there is an aptitude of type Activity Area"""

        aptitudes = aptitudes_column.toPython()
        n_aptitudes = len(aptitudes)

        if n_aptitudes == 0:
            return False

        for ap in aptitudes:
            if ap.get("type", None) == "ACTIVITY_AREA":
                return True

        return False

    @staticmethod
    def no_free_text_skills(aptitudes_column):

        if aptitudes_column is None:
            return False
        
        """Checks if there is an without conceptId"""

        aptitudes = aptitudes_column.toPython()
        n_aptitudes = len(aptitudes)

        if n_aptitudes == 0:
            return False

        for ap in aptitudes:
            if ap.get("conceptId", None) is None:
                return False
        return True
    

    @staticmethod
    def proficiency_on_twenty_pct(aptitudes_column):

        if aptitudes_column is None:
            return False
        
        """Checks if at least 20% of aptitudes have a proficiency greater than 0"""

        aptitudes = aptitudes_column.toPython()
        n_aptitudes = len(aptitudes)

        if n_aptitudes == 0:
            return False

        apt_gt0_count = 0
        for ap in aptitudes:
            if ap.get("proficiency", None):
                if ap.get("proficiency", None) > 0:
                    apt_gt0_count += 1

        if apt_gt0_count/n_aptitudes >= 0.2:
            return True
        return False

    @staticmethod
    def all_skills_categorized(aptitudes_column):

        if aptitudes_column is None:
            return False
        
        """Checks if there is all aptitudes are not UNCLASSIFIED"""

        aptitudes = aptitudes_column.toPython()
        n_aptitudes = len(aptitudes)

        if n_aptitudes == 0:
            return False

        for ap in aptitudes:
            if ap.get("type", None) == "UNCLASSIFIED":
                return False
        return True

    @staticmethod
    def education_field_filled(educations_column):

        if educations_column is None:
            return False

        """Checks if all educations have the mandatory fields."""

        educations = educations_column.toPython()
        n_educations = len(educations)

        if n_educations == 0:
            return False

        mandatory_fields = ["degree", "school", "startDate", "endDate"]
        for education in educations:
            for f in mandatory_fields:
                if not education.get(f, None):
                    return False
        return True

    @staticmethod
    def bio_word_count(headline_col):

        if headline_col is None:
            return False

        range_check = lambda count: count >= 30 and count <= 150

        headline = headline_col.toPython()

        if headline.get("bio", None):
            return range_check(simple_word_counter(headline.get("bio")))
        return False

    @staticmethod
    def skills_linked_to_positions(aptitudes_col, positions_col):
        """Checks if at least 20% of position skills are in profile skills."""
        if aptitudes_col is None or positions_col is None:
            return False
        aptitudes = aptitudes_col.toPython()
        positions = positions_col.toPython()

        if len(aptitudes) == 0 or len(positions) == 0:
            return False

        ap_ids = [ap.get("id", None) for ap in aptitudes if ap.get("id", None) is not None]

        positions_unique_skills = set()

        for p in positions:
            aps = p.get("aptitudeReferences", None)
            if aps:
                for ap in aps:
                    ap_id = ap.get("aptitudeId", None)
                    if ap_id:
                        positions_unique_skills.add(ap_id)

        pos_skill_counter = 0
        for ap_id in positions_unique_skills:
            if ap_id in ap_ids:
                pos_skill_counter += 1

        if pos_skill_counter/len(ap_ids) >= 0.2:
            return True

        return False

    def add_profile_score(self, profile_df):
        cm = self.conditions_map

        ndf = profile_df
        for k, v in cm.items():
            f = F.udf(v["function"], ty.BooleanType())
            ndf = ndf.withColumn(
                f"{k}_{v['name']}",
                f(
                    *[F.col(con) for con in v["columns"]]
                )
            )

        top_c = ["c1", "c2", "c3", "c4", "c5", "c6"]

        top_priority_score = reduce(
            lambda x, y: x+y,
            [
                F.coalesce(
                    F.col(f"{cond}_{cm[cond]['name']}").cast("int"), 
                    F.lit(0)
                ) for cond in top_c
            ]
        )

        weighted_score = reduce(
            lambda x, y: x+y,
            [
                cm[cond]["weight"]*F.coalesce(
                    F.col(f"{cond}_{cm[cond]['name']}").cast("int"), 
                    F.lit(0)
                ) for cond in cm.keys()
            ]
        )

        raw_score = F.round((weighted_score / self.weight_sum)*100)

        top_priority_multiplier = {
            0: 0.1,
            1: 0.4,
            2: 0.55,
            3: 0.7,
            4: 0.82,
            5: 0.92,
            6: 1
        }

        top_priority_multiplier_map = F.create_map(
            *[x for k,v in top_priority_multiplier.items() for x in (F.lit(k), F.lit(v))]
        )

        score = F.round(raw_score*top_priority_multiplier_map[top_priority_score])

        ndf = ndf.withColumn(
            "profileScore",
            score
        ).withColumn(
            f"profileScore_Status",
            F.when(F.col("profileScore") > 60, "Completed")
                .otherwise("Not completed")
        )

        return ndf

    # End of class


def add_profile_score_f(profile_df):
    return ProfileScoreNamespace().add_profile_score(profile_df)


# raw_reader.py



import pyspark.pipelines as dp
import pyspark.sql.functions as F
from pathlib import Path
import pyspark.sql.types as ty
# import common.utils as ut

import cloudpickle
cloudpickle.register_pickle_by_value(sys.modules[__name__])


class RawReader(): # Hardcoded everywhere cause imports dont work

    @staticmethod
    def volume_path_maker(catalog: str, read_schema: str, volume_name: str):
        return Path(f"/Volumes/{catalog}/{read_schema}/{volume_name}/")

    @staticmethod
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
            return (date_str[:10], None)
        elif validity_count < 12 and validity_count > 8:
            return (None, "Invalid date.")
        else:
            return (None, "Date not provided.")
        return None

    
    @staticmethod
    def raw_json_reader(sparkSession,
                        volume_path: str,
                        glob_regex: str,
                        streaming: bool = True):
        
        """Reads from volume expecting a list of jsons, 
        
        Returns a table with load metadata and a column named payload.
        
        The streaming table, considers a new input when a new file arrives.
        Does not bother checking changes.
        """


        if streaming:
            reader = sparkSession.readStream\
                                .format("cloudFiles")\
                                .option("cloudFiles.format", "json")\
            # No need to give spark permission to infer schema because,
            # we store the data as payload and we do it manually since spark's is not enough
        else:
            reader = sparkSession.read.format("json")

        df = reader.option("multiLine", "true")\
                    .option("singleVariantColumn", "payload")\
                    .option("pathGlobFilter", glob_regex)\
                    .load(str(volume_path))\
                    .select(
                        "payload",
                        F.col("_metadata.file_path").alias("_source_file"),
                        F.col("_metadata.file_name").alias("_source_file_name"),
                        F.col("_metadata.file_size").alias("_source_file_size"),
                        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
                    ).lateralJoin(
                        sparkSession.tvf.variant_explode(F.col("payload").outer()) # Outer is needed if you want to use this expression in a pipeline
                    ).select(
                        F.col("value").alias("payload"),
                        "_source_file",
                        "_source_file_name",
                        "_source_file_size",
                        "_source_file_modified_at",
                    )

        # We add the snapshot date and the error if any as a column
        schema = ty.StructType([
            ty.StructField("date", ty.StringType(), True),
            ty.StructField("file_name_error", ty.StringType(), True),
        ])
        get_date = F.udf(RawReader.get_date_and_error, schema)
        df = (
            df
            .withColumn("resultado", get_date(F.col("_source_file_name")))
            .withColumn("snapshot_ts", F.to_date("resultado.date", "yyyy-MM-dd"))
            .withColumn("file_name_error", F.col("resultado.file_name_error"))
            .withColumn("ingest_ts", F.current_timestamp())
            .drop("resultado")
        )


        return df

def raw_bronze_pipe_maker(spark, catalog, platform, table_name, streaming=False):

    platform_name = get_platform(spark, platform)
    target_quality = get_quality(spark, "bronze")

    target_schema = schema_name_builder(target_quality, platform_name)
    target_table = table_name_builder("raw", table_name)
    target_path = f"{catalog}.{target_schema}.{target_table}"
    read_volume = get_table_volume(spark, "data", platform, table_name)

    files_glob_regex = get_table_glob_regex(spark, platform, table_name)


    @dp.table(
        name = target_path,
        comment=(
            f"Raw {table_name}. Read from its JSON and loaded as payload column with metadata columns, aswell as snapshot date added."
        ),
        table_properties={
            "quality": target_quality
        }
    )
    def f():
        return RawReader.raw_json_reader(spark, read_volume, files_glob_regex, streaming)


    # We add the error derivation to quarantine table
    quarantine_schema = get_quarantine_schema(spark)
    quarantine_table = get_quarantine_table(spark, "files")
    quarantine_path = f"{catalog}.{quarantine_schema}.{quarantine_table}"
    @dp.append_flow(
        target=quarantine_path,
        name=f"flow_{target_path.replace('.','_')}_{quarantine_path.replace('.', '_')}"
    )
    def f2():
        quarantine_df = spark.readStream.table(target_path).where(F.col("snapshot_ts").isNull()).select(
            "_source_file",
            "_source_file_name",
            "snapshot_ts",
            "file_name_error"
        )
        return quarantine_df

    return None


def create_quarantine_table(spark, catalog):
    """Needs to be called before pipe maker. Creates the streaming table"""
    quarantine_schema = get_quarantine_schema(spark)
    quarantine_table = get_quarantine_table(spark, "files")

    dp.create_streaming_table(
        name=f"{catalog}.{quarantine_schema}.{quarantine_table}",
        comment="Quarantine table. Holds error in file names."
    )
    return None


# payload_to_table.py



import pyspark.pipelines as dp
import pyspark.sql.functions as F
import pyspark.sql.types as ty
# import common.utils as ut
# import common.constants as c
from pathlib import Path
# This file has all the functions neccessary to translate a payload variant column from raw table into a bronze table.

import cloudpickle
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def payload_subset_expr_string_maker(variant_col_name, cols_to_select):
    expr_string = "object_construct("
    n_cols = len(cols_to_select)
    for i, col in enumerate(cols_to_select):
        if i+1 == n_cols:
            end_str = ""
        else:
            end_str = ","
        expr_string = expr_string + f"'{col}', {variant_col_name}:{col}{end_str}"

    expr_string = expr_string + ")"
    return expr_string


def parse_data_type(dt_):
    if dt_ == "DATE":
        return "STRING"
    return dt_

@F.udf(returnType=ty.ArrayType(ty.StringType()))
def get_variant_keys(variant):
    if variant is None:
        return None

    value = variant.toPython()

    if isinstance(value, dict):
        return list(value.keys())

    return list()


def transform_payload_to_table(og_df, known_schema, metadata_cols):
    """
    Takes the og_df that is meant to have only payload column and metadata.

    Takes known_schema that is a dict with the shape (column: {type: str, format: str})

    Selects the known schema columns and transform it to columns, and the remaining payload json is kept in the column remaining_payload
    """
    cols = list()

    for field_name, data_type in known_schema.items():
        if data_type["type"] == "VARIANT":
            col = (
                F.parse_json(F.expr(f"variant_get(payload, '$.{field_name}', 'string')"))
                .cast(parse_data_type(data_type["type"]))
                .alias(field_name)
            )
        else:
            col = (
                F.expr(f"variant_get(payload, '$.{field_name}', 'string')")
                .cast(parse_data_type(data_type["type"]))
                .alias(field_name)
            )
        cols.append(col)
        """
        if data_type["type"] == "VARIANT":
            col2 = (
                F.expr(f"xxhash64(variant_get(payload, '$.{field_name}', 'string'))")
                .cast(parse_data_type("STRING"))
                .alias(f"hash_{field_name}")
            )
            cols.append(col2)
        """

    cols.extend(metadata_cols)
    cols.append(SNAPSHOT_COL)
    cols.append(INGEST_TS_COL)

    df = og_df.withColumn(
            "remaining_keys",
            F.array_except(
                get_variant_keys("payload"),
                F.array(*[F.lit(k) for k in known_schema.keys()])
            )
        ).withColumn(
            "remaining_payload",
            F.expr("""
                to_variant_object(
                    map_from_arrays(
                        transform(
                            remaining_keys,
                            k -> k
                        ),
                        transform(
                            remaining_keys,
                            k -> variant_get(payload, '$.k', 'string')
                        )
                    )
                )
            """)
        )

    cols.append("remaining_payload")
    return df.select(*cols)




def payload_top_level_to_table_pipe_maker(spark, catalog, platform, table_name):

    platform_name = get_platform(spark, platform)
    target_quality = get_quality(spark, "bronze")
    target_schema = schema_name_builder(target_quality, platform_name)

    read_table = table_name_builder("raw", table_name)
    target_table = table_name_builder("base", table_name)
    read_path = f"{catalog}.{target_schema}.{read_table}"
    target_path = f"{catalog}.{target_schema}.{target_table}"
    known_schema_vol = get_table_volume(spark, "schema", platform, table_name)
    known_schema_file = get_table_metadata_files(spark, "schema", platform, table_name)
    known_schema_path = Path(known_schema_vol) / Path(known_schema_file)

    @dp.table(
        name=target_path,
        comment=f"Base Bronze {table_name}. Contains the top level payload keys as columns and formatted when possible.",
        table_properties={
            "quality":target_quality
        }
    )
    def f():
        df = spark.readStream.table(read_path)
        known_schema = load_json(known_schema_path)
        metadata_cols = METADATA_COLUMNS
        if table_name == "profile":
            ndf = add_bucket_and_status(
                add_profile_score_f(
                    transform_payload_to_table(df, known_schema, metadata_cols)
                ), 
                "completionRate")
            return ndf
        
        return transform_payload_to_table(df, known_schema, metadata_cols)

    return None


# snapshot_processor.py



import pyspark.sql as sql
import pyspark.sql.functions as F
import pyspark.pipelines as dp
# import common.constants as c
# import common.utils as ut
from pathlib import Path

def remove_hashed_cols(list_of_cols):
    "variant cols have a hash equivalent, the variant cols cant be accounted for when using dropDuplicates, so we remove those cols from the subset of cols to dropDuplicates"

    cols_with_hash = list()
    for c in list_of_cols:
        if startswith("hash_"):
            cols_with_hash.append(c[len("hash_"):])

    cols = [c for c in list_of_cols if c not in cols_with_hash]

    return cols




def last_snapshot_filter(snapshot_df, partition_cols, extra_cols_to_skip=None):

    # Extra cols to skip, cols that dont want to include in subset during dropDuplicates
    snapshot_ts_col = SNAPSHOT_COL
    # First we drop duplicates on all columns except the snapshot_dates one
    cols = snapshot_df.columns

    if extra_cols_to_skip is None:
        extra_cols_to_skip = list()

    if not isinstance(extra_cols_to_skip, list):
        raise TypeError("extra cols must be a list.")

    if not isinstance(partition_cols, (str, list)):
        raise TypeError("partition cols must be a list or a str")

    if isinstance(partition_cols, str):
        partition_cols = [partition_cols]

    if not all_in(partition_cols, cols):
        raise ValueError(f"partition columns ({partition_cols}, {cols}) are not in columns.")

    if snapshot_ts_col not in cols:
        raise ValueError(f"snapshot timestamp column ({snapshot_ts_col}) is not in columns.")

    
    cols_without_snapshot_ts = [c for c in cols if c != snapshot_ts_col and c not in extra_cols_to_skip]

    newdf = snapshot_df.dropDuplicates(subset=remove_hashed_cols(cols_without_snapshot_ts))

    window = sql.Window.partitionBy(*partition_cols).orderBy(snapshot_ts_col)

    newdf = newdf.withColumn(
        "last_snapshot",
        F.max(
            F.col(snapshot_ts_col)
        ).over(window)
    ).withColumn(
        "row_number", 
        F.row_number().over(window)
    ).filter(
        (F.col(snapshot_ts_col) == F.col("last_snapshot"))
        & (F.col("row_number") == 1) # We choose 1 cause it should not matter if it is the first or last, and we won't bother calculating the last
    ).drop("row_number").drop("last_snapshot")
    


    return newdf


def history_table_maker(snapshot_df, cols_to_keep, dates_filter_expr):

    cols = snapshot_df.columns

    if not isinstance(cols_to_keep, list):
        raise ValueError("columns to keep must be a list.")

    if not all_in(cols_to_keep, cols):
        raise ValueError(f"cols to keep ({cols_to_keep}, {cols}) are not in columns.")

    cols_to_keep_2 = list(cols_to_keep)
    cols_to_keep_2.append(SNAPSHOT_COL)
    df = snapshot_df.where(dates_filter_expr.replace("snapshot_ts", SNAPSHOT_COL)).select(*cols_to_keep_2)

    return df


def to_list(val):
    if isinstance(val, list):
        return val
    return [val]

def snapshot_pipe_maker(spark, catalog, platform, table_name):

    platform_name = get_platform(spark, platform)
    target_quality = get_quality(spark, "bronze")
    target_schema = schema_name_builder(target_quality, platform_name)

    read_table = table_name_builder("base", table_name)
    current_table = table_name_builder("current", table_name)
    history_table = table_name_builder("history", table_name)
    read_path = f"{catalog}.{target_schema}.{read_table}"
    current_table_path = f"{catalog}.{target_schema}.{current_table}"
    history_table_path = f"{catalog}.{target_schema}.{history_table}"

    dates_filter_expr = get_conf(spark, "history.filter_expression")

    default_config_file_name = get_conf(spark, "file.default_config")
    table_config_file_name = get_table_metadata_files(spark, "config", platform, table_name)
    table_config_volume = get_table_volume(spark, "config", platform, table_name)
    table_config_path = Path(table_config_volume) / table_config_file_name
    default_config_path = Path(table_config_volume) / default_config_file_name
    table_config = get_table_config(table_config_path, platform, table_name, default_config_path)
    default_config = load_json(default_config_path)

    dp.create_streaming_table(
        name=current_table_path,
        comment=f"Last snapshot {table_name} data.",
        table_properties={
            "quality": target_quality
        },
        expect_all_or_drop={"not_null_snapshot_date": f"{SNAPSHOT_COL} IS NOT NULL"}
    )
    #@dp.expect_or_drop("not_null_snapshot_date", f"{SNAPSHOT_COL} IS NOT NULL")
    #def f():
        #pass

    dp.create_auto_cdc_flow(
        target=current_table_path,
        source=read_path,
        keys=to_list(table_config.get("last_snapshot", default_config["last_snapshot"])["partition_cols"]),
        sequence_by=SNAPSHOT_COL
    )
    """
    def f():
        df = spark.readStream.table(read_path)
        current_df = last_snapshot_filter(df, 
                                          table_config["last_snapshot"]["partition_cols"], 
                                          table_config["last_snapshot"]["extra_cols_to_skip"])
        return current_df
    """

    if table_config.get("history", None) is not None:
        @dp.table(
            name=history_table_path,
            comment=f"Required time series {table_name} data.",
            table_properties={
                "quality": target_quality
            }
        )
        @dp.expect_or_drop("not_null_snapshot_date", f"{SNAPSHOT_COL} IS NOT NULL")
        def f2():
            df = spark.readStream.table(read_path)
            history_df = history_table_maker(df, table_config["history"]["cols_to_keep"], dates_filter_expr)
            return history_df

    return None


# basic_filtering_and_quarantine.py



import pyspark.sql as sql
import pyspark.sql.functions as F
import pyspark.pipelines as dp
# import common.utils as ut
from pathlib import Path




_quarantine_sanitize_created = False

def create_quarantine_table_sanitize(spark, catalog):
    """Creates the streaming table that will hold the data that needs to be sanitize. Only creates it once."""
    global _quarantine_sanitize_created
    if _quarantine_sanitize_created:
        return None
    _quarantine_sanitize_created = True
    quarantine_schema = get_quarantine_schema(spark)
    quarantine_table = get_quarantine_table(spark, "sanitize")

    dp.create_streaming_table(
        name=f"{catalog}.{quarantine_schema}.{quarantine_table}",
        comment="Quarantine table. Holds error rows."
    )
    return None
# For silver accreditation we just need to extract the  aptitudes (skills) and ensure some quality.

def silver_quality_pipe_maker(spark, catalog, platform, table_name):
    # Como en la fase silver evitamos agregaciones, extraemos la tabla que contiene la relación, accreditación, profile, skill

    platform_name = get_platform(spark, platform)

    read_quality = get_quality(spark, "bronze")
    read_schema = schema_name_builder(read_quality, platform_name)
    target_quality = get_quality(spark, "silver")
    target_schema = schema_name_builder(target_quality, platform_name)

    read_table = table_name_builder("current", table_name)
    target_table = table_name_builder("cleaned", table_name)
    read_path = f"{catalog}.{read_schema}.{read_table}"
    target_path = f"{catalog}.{target_schema}.{target_table}"

    default_config_file_name = get_conf(spark, "file.default_config")
    table_config_file_name = get_table_metadata_files(spark, "config", platform, table_name)
    table_config_volume = get_table_volume(spark, "config", platform, table_name)
    table_config_path = Path(table_config_volume) / table_config_file_name
    default_config_path = Path(table_config_volume) / default_config_file_name
    table_config = get_table_config(table_config_path, platform, table_name, default_config_path)
    silver_config = table_config.get("silver", None)

    if silver_config is not None:
        cols_to_drop = silver_config.get("cols_to_drop", None)
        drop_metadata = silver_config.get("drop_metadata", True)
        filter_queries = silver_config.get("constraints", None)
        variant_keys_to_extract = silver_config.get("variant_keys_to_extract", None)
        quarantine_queries = silver_config.get("quarantine", None)
        middle_table = silver_config.get("middle_table", None)
        pk_cols = silver_config.get("pk_cols", None)


        @dp.table(
            name=target_path,
            comment=f"{table_name} cleaned and filtered data.",
            table_properties={
                "quality": target_quality
            }
        )
        def f():
            df = spark.readStream.table(read_path)
            if variant_keys_to_extract is not None:
                if not isinstance(variant_keys_to_extract, dict):
                    raise TypeError("variant keys to extract must be a dict. Col: Dict[key, format]")
                for col, keys_format in variant_keys_to_extract.items():
                    df = extract_variant_keys(df, col, keys_format)
                
            if filter_queries is not None:
                df = filter_conditions(df, filter_queries)

            if cols_to_drop is not None:
                df = remove_cols(df, cols_to_drop, drop_metadata)

            if table_name == "talent":
                df = add_seniority(df, "yearsOfExperience")
                df = add_date_status(df, "lastConnectionDate")

            #if table_name == "profile":
                #df = add_bucket_and_status(df, "completionRate")

            if table_name == "collab_status":
                df = classify_status_code(spark, df, "status")
            return df

        if quarantine_queries is not None:
            create_quarantine_table_sanitize(spark, catalog)
            quarantine_schema = get_quarantine_schema(spark)
            quarantine_table = get_quarantine_table(spark, "sanitize")
            quarantine_path = f"{catalog}.{quarantine_schema}.{quarantine_table}"
            if pk_cols is None:
                raise ValueError("PK Columns must be provided in table quality config. It is missing in the json file.")
            @dp.append_flow(
                target=quarantine_path,
                name=f"flow_{target_path.replace('.','_')}_{quarantine_path.replace('.','_')}"
            )
            def f2():
                df = spark.readStream.table(read_path)
                return make_quarantine_table(df, quarantine_queries, pk_cols, read_path)

        if middle_table is not None:

            pks = middle_table.get("pks", None)
            cols_to_extract = middle_table.get("cols_to_extract", None)
            new_pk_names = middle_table.get("pk_new_names", None)
            extracted_cols_names = middle_table.get("extracted_cols_names", None)
            variant_paths = middle_table.get("variant_paths", None)


            if None in [pks, cols_to_extract, new_pk_names, extracted_cols_names]:
                raise ValueError("Missing any of these keys: pks, col_to_extract, pk_new_names, extracted_col_name, extraction_name")

            if isinstance(cols_to_extract, list):
                extraction_names = middle_table.get("extraction_names", None)
                if extraction_names is None:
                    raise ValueError("extraction_names is missing")
                
                if (not isinstance(extracted_cols_names, list) 
                    or not (isinstance(variant_paths, list) or variant_paths is None)
                    or not isinstance(extraction_names, list)
                    ):
                    raise TypeError("If there are multiple cols_to_extract, extracted_cols_names, varian_paths (if provided) and extraction_name must be a list, each one referring to their respective column_to_extract")
                c1 = len(cols_to_extract) != len(extracted_cols_names)
                c2 = len(cols_to_extract) != len(extraction_names)
                c3 = False
                if variant_paths is not None:
                    c3 = len(cols_to_extract) != len(variant_paths)

                if (c1 or c2 or c3):
                    raise ValueError("Len of cols_to_extract, extracted_cols_names and variant_paths (if provided) must match if cols_to_extract is a list.")
                
                for i, col_to_extract in enumerate(cols_to_extract):
                    extraction_name = extraction_names[i]
                    middle_table_name = f"{table_name}_{extraction_name}"
                    middle_table_path = f"{catalog}.{target_schema}.{middle_table_name}"
                    if variant_paths is None:
                        @dp.table(
                            name=middle_table_path,
                            comment=f"Multi-valued attribute from {table_name}, attribute {extraction_name}",
                            table_properties={
                                "quality": target_quality
                            }
                        )
                        def f3():
                            df = spark.readStream.table(read_path)
                            return middle_table_extractor(spark, df, pks, col_to_extract, new_pk_names, extracted_cols_names[i], None)
                    else:
                        @dp.table(
                            name=middle_table_path,
                            comment=f"Multi-valued attribute from {table_name}, attribute {extraction_name}",
                            table_properties={
                                "quality": target_quality
                            }
                        )
                        def f3():
                            df = spark.readStream.table(read_path)
                            return middle_table_extractor(spark, df, pks, col_to_extract, new_pk_names, extracted_cols_names[i], variant_paths[i])
            else:
                extraction_name = middle_table.get("extraction_names", None)
                middle_table_name = f"{table_name}_{extraction_name}"
                middle_table_path = f"{catalog}.{target_schema}.{middle_table_name}"

                @dp.table(
                    name=middle_table_path,
                    comment=f"Multi-valued attribute from {table_name}, attribute {extraction_name}",
                    table_properties={
                        "quality": target_quality
                    }
                )
                def f3():
                    df = spark.readStream.table(read_path)
                    return middle_table_extractor(spark, df, pks, cols_to_extract, new_pk_names, extracted_cols_names, variant_paths)
            

    return None


# all_tables_bronze_silver.py


# import common.raw_processing.raw_reader as rr
# import common.raw_processing.payload_to_table as pt
# import common.raw_processing.snapshot_processor as sp
# import common.silver_processing.basic_filtering_and_quarantine as bfq
# import common.utils as ut


PLATFORM_TABLES = {
    "analytic": ["bu", "department", "entity", "service_line", "site", "society"],
    "whoz": ["accreditation", "certification", "profile", "skill", "talent", "user"],
    "perso": ["collab_status", "leave", "workers"]
}
CATALOG = get_conf(spark, "catalog")
create_quarantine_table(spark, CATALOG)
#create_quarantine_table_sanitize(spark, CATALOG)
for platform, tables in PLATFORM_TABLES.items():
    for table in tables:
        raw_bronze_pipe_maker(spark, CATALOG, platform, table, True)
        payload_top_level_to_table_pipe_maker(spark, CATALOG, platform, table)
        snapshot_pipe_maker(spark, CATALOG, platform, table)
        silver_quality_pipe_maker(spark, CATALOG, platform, table)

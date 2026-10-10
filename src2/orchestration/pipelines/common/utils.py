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
import common.constants as c
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


def explode_variant_list_column(sparkSession, df, col_to_explode, remove_row_if_empty_list: bool = False, redo_og: bool = False):
    """Returns the same df but with the col_to_explode column exploded"""
    if col_to_explode not in df.columns:
        raise ValueError(f"{col_to_explode} not in df columns: {df.columns}")

    if remove_row_if_empty_list:
        f = sparkSession.tvf.variant_explode
    else:
        f = sparkSession.tvf.variant_explode_outer
    
    ndf = df.lateralJoin(
        f(F.col(col_to_explode).outer()) # Outer is needed if you want to use this expression in a pipeline
    )

    if redo_og:
        ndf = ndf.drop(F.col(col_to_explode), F.col("key"), F.col("pos")).withColumnRenamed("value", col_to_explode)
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
        cols_to_remove.extend(c.METADATA_COLUMNS)
        cols_to_remove.append(c.SNAPSHOT_COL)
        cols_to_remove.append(c.INGEST_TS_COL)
        cols_to_remove.append(c.KNOWN_SNAPSHOT)
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
    ndf = df
    for con in list_of_constraints:
        ndf = ndf.where(con)

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
             .when(F.col(column) <= 1, "0.8-1")
             .otherwise(None)
        )
        .withColumn(
            f"{column}_Status",
            F.when(F.col(column) >= 0.8, "Completed")
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


def add_date_status_prof(df, column: str):
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
             .when(days_since < 30, "Last month")
             .when(days_since < 90, "Last 3 months")
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
                "weight": 10.7 #15
            },
            "c2": {
                "function": self.positions_have_skills,
                "name": "positions_have_skills",
                "columns": ["positions"],
                "weight": 10.7 #15
            },
            "c3": {
                "function": self.assignments_have_skills,
                "name": "assignments_have_skills",
                "columns": ["positions"],
                "weight": 10.7 #15
            },
            "c4": {
                "function": self.short_project_context,
                "name": "short_project_context",
                "columns": ["positions"],
                "weight": 10.7 #15
            },
            "c5": {
                "function": self.favourite_skills,
                "name": "favourite_skills",
                "columns": ["aptitudes"],
                "weight": 10.7 #15
            },
            "c6": {
                "function": self.profile_updated,
                "name": "profile_updated",
                "columns": ["lastModifiedDate", c.SNAPSHOT_COL],
                "weight": 10.7 #15
            },
            "c7": {
                "function": self.position_fields_filled,
                "name": "position_fields_filled",
                "columns": ["positions"],
                "weight": 2.1 #3
            },
            "c8": {
                "function": self.assignment_fields_filled,
                "name": "assignment_fields_filled",
                "columns": ["positions"],
                "weight": 2.1 #3
            },
            "c9": {
                "function": self.no_forbidden_words,
                "name": "no_forbidden_words",
                "columns": ["positions"],
                "weight": 2.1 #3
            },
            "c10": {
                "function": self.has_activity_area,
                "name": "has_activity_area",
                "columns": ["aptitudes"],
                "weight": 5.7 #8
            },
            "c11": {
                "function": self.no_free_text_skills,
                "name": "no_free_text_skills",
                "columns": ["aptitudes"],
                "weight": 5.7 #8
            },
            "c12": {
                "function": self.proficiency_on_twenty_pct,
                "name": "proficiency_on_twenty_pct",
                "columns": ["aptitudes"],
                "weight": 5.7 #8
            },
            "c13": {
                "function": self.all_skills_categorized,
                "name": "all_skills_categorized",
                "columns": ["aptitudes"],
                "weight": 2.1 #3
            },
            "c14": {
                "function": self.education_field_filled,
                "name": "education_field_filled",
                "columns": ["educations"],
                "weight": 2.1 #3
            },
            "c15": {
                "function": self.bio_word_count,
                "name": "bio_word_count",
                "columns": ["headline"],
                "weight": 2.1 #3
            },
            "c16": {
                "function": self.skills_linked_to_positions,
                "name": "skills_linked_to_positions",
                "columns": ["aptitudes","positions"],
                "weight": 5.7 #8
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

        #score = F.round(raw_score*top_priority_multiplier_map[top_priority_score])
        score = F.round(weighted_score)

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



def extract_english_terms(sparkSession, skill_df, col):
    "extracts the english term from the skill column"
    exploded = explode_variant_list_column(sparkSession, skill_df, col)
    exploded = exploded.drop(F.col(col), F.col("pos"), F.col("key"), F.col(col)).withColumnRenamed("value", col)
    extracted = extract_variant_keys(exploded, col, {"language": "STRING", "text": "STRING"})
    filtered = extracted.where(
        (F.col(f"{col}_language") == "en")
        | (F.col(f"{col}_language").isNull())
    )
    return filtered

def extract_all_english_terms(sparkSession, skill_df):
    ndf = skill_df
    english_cols = ["name", "description", "terms", "hiddenTerms", "wikipediaLink", "depiction"]
    for c in english_cols:
        ndf = extract_english_terms(sparkSession, ndf, c)
    return ndf


def custom_join(left_df, right_df, alias_left, alias_right, join_on_cond, how_mode):

    l = left_df.alias(alias_left)
    r = right_df.alias(alias_right)

    l_cols = l.columns
    r_cols = r.columns

    left_cols = list()

    for col in l_cols:
        if col in r_cols:
            left_cols.append(
                l[col].alias(f"{alias_left}_{col}")
            )
        else:
            left_cols.append(
                l[col].alias(f"{col}")
            )

    
    right_cols = list()

    for col in r_cols:
        if col in l_cols:
            right_cols.append(
                r[col].alias(f"{alias_right}_{col}")
            )
        else:
            right_cols.append(
                r[col].alias(f"{col}")
            )

    ndf = l.join(
        r,
        join_on_cond,
        how=how_mode
    ).select(
        *left_cols,
        *right_cols
    )

    return ndf


_quarantine_sanitize_created = False

def create_quarantine_table_sanitize(spark, catalog):
    """Creates the streaming table that will hold the data that needs to be sanitize. Only creates it once."""
    global _quarantine_sanitize_created
    if _quarantine_sanitize_created:
        return None
    _quarantine_sanitize_created = True
    quarantine_schema = ut.get_quarantine_schema(spark)
    quarantine_table = ut.get_quarantine_table(spark, "sanitize")

    dp.create_streaming_table(
        name=f"{catalog}.{quarantine_schema}.{quarantine_table}",
        comment="Quarantine table. Holds error rows."
    )
    return None


def max_filter(df, pk_cols, col_to_select, temp_view_name):

    if not isinstance(pk_cols, (str, list)):
        raise TypeError("pk_cols must be a list or str")

    if not isinstance(col_to_select, str):
        raise TypeError("col_to_select must be a str.")

    if isinstance(pk_cols, str):
        pk_cols = [pk_cols]

    if not all(c in df.columns for c in pk_cols) or col_to_select not in df.columns:
        raise ValueError("pk_cols and col_to_select must be in df columns.")

    cols = [c for c in df.columns if c not in pk_cols]

    max_df = df.groupBy(pk_cols).agg(F.max(F.col(col_to_select)).alias("max_"))

    df.createOrReplaceTempView(temp_view_name)
    max_df.createOrReplaceTempView(temp_view_name + "_max")
    """
    # Join con el dataframe original para obtener las filas completas
    ndf = df.join(
        max_,
        on=pk_cols
    ).filter(
        F.col(col_to_select) == F.col("max_")
    ).drop("max_")
    """

    left_alias = "og"
    right_alias = "m"
    join_cols_sql = " AND ".join(f"{left_alias}.{pk} = {right_alias}.{pk}" for pk in pk_cols)

    df_cols = df.columns
    select_cols_sql = ", ".join(f"{left_alias}.{col} AS {col}" for col in df_cols)
    
    # We need to creat the temp view and use sql
    ndf = spark.sql(f"SELECT {select_cols_sql} FROM {temp_view_name} AS {left_alias} LEFT JOIN {temp_view_name + "_max"} AS {right_alias} ON {join_cols_sql} WHERE {left_alias}.{col_to_select} = {right_alias}.max_")
    return ndf


def make_department_service_line_zones(sparkSession, department_df, service_line_df):
    dep_sl = explode_variant_list_column(sparkSession, department_df, "associated_service_line", False, True)
    dep_sl_zone = explode_variant_list_column(sparkSession, dep_sl, "associated_zone", False, True).withColumn("associated_service_line", F.col("associated_service_line").cast("int")).withColumn("associated_zone", F.col("associated_zone").cast("int"))

    sl_dep = explode_variant_list_column(sparkSession, service_line_df, "associated_practice", False, True)
    sl_dep_zone = explode_variant_list_column(sparkSession, sl_dep, "associated_zone", False, True).withColumn("associated_practice", F.col("associated_practice").cast("int")).withColumn("associated_zone", F.col("associated_zone").cast("int"))

    ndf = custom_join(
        dep_sl_zone,
        sl_dep_zone,
        "department", "service_line",
        (
            (dep_sl_zone["associated_zone"] == sl_dep_zone["associated_zone"])
            & (dep_sl_zone["associated_service_line"] == sl_dep_zone["id"])
            & (dep_sl_zone["id"] == sl_dep_zone["associated_practice"])
        ),
        "inner"
    )

    zone_names = spark.read.csv(get_conf(spark, "file.encode.zone_map"),
                                header=True,
                                inferSchema=True)
    bucu_dep_map = spark.read.csv(get_conf(spark, "file.encode.department_bucu"),
                                header=True,
                                inferSchema=True)
    dep_zone_map = spark.read.csv(get_conf(spark, "file.encode.department_zone_map"),
                                header=True,
                                inferSchema=True)

    ndf = custom_join(
        ndf, zone_names,
        "d_sl_z", "zone_map",
        ndf["department_associated_zone"] == zone_names["id"],
        "left"
    )

    ndf = custom_join(
        ndf, bucu_dep_map,
        "d_sl_z_2", "bucu",
        ndf["department_name"] == bucu_dep_map["practiceName"],
        "left"
    )

    ndf = ndf.fillna({"BUCU": "BU"})

    ndf = custom_join(
        ndf, dep_zone_map,
        "d_sl_z_3", "dz",
        ndf["department_name"] == dep_zone_map["department_name"],
        "left"
    )

    return ndf


def map_site(sparkSession, site_df):

    site_maps = spark.read.csv(get_conf(sparkSession, "file.encode.site_maps"),
                                header=True,
                                inferSchema=True)

    ndf = custom_join(site_df, site_maps, "site", "site_map", site_df["name"]==site_maps["site_name"], "left")

    ndf = ndf.withColumn("id", F.col("id").cast("int"))
    ndf = ndf.withColumn("active", F.col("active").cast("int"))
    return ndf


def add_is_active_col(df, end_date_col, is_active_col_name="is_active"):
    ndf = df.withColumn(
        is_active_col_name,
        F.when(
            F.col(end_date_col).isNull(),
            F.lit(False)
        ).otherwise(
            F.current_date() > F.col(end_date_col)
        )
    )
    return ndf


def trim_str_cols(df):
    for field in df.schema:
        if isinstance(field.dataType, ty.StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def add_last_mission_col_to_profile_df(profile_df, profile_positions_df):

    pp_df = profile_positions_df.where("is_mission = TRUE")

    last_pos_date = pp_df.groupBy("profile_id").agg(F.max("startDate").alias("last_date")).withColumnRenamed("profile_id", "pp_df")
    pp_df = pp_df.join(last_pos_date, pp_df["profile_id"] == last_pos_date["pp_df"], "left").drop("pp_df")


    pp_df = pp_df.where("startDate = last_date").drop("last_date")

    pp_count = pp_df.groupBy("profile_id").count().withColumnRenamed("profile_id", "c_profile_id")
    pp_df = pp_df.join(pp_count, pp_df["profile_id"] == pp_count["c_profile_id"], "left").drop("c_profile_id")

    g1 = pp_df.where("count = 1")
    g2 = pp_df.where("count > 1")

    g3 = g2.where("endDate IS NULL")
    g4 = g2.where("endDate IS NOT NULL").join(g3.select("profile_id"), on="profile_id", how="left_anti")

    last_end_date = g4.groupBy("profile_id").agg(F.max("endDate").alias("last_end_date")).withColumnRenamed("profile_id", "l_profile_id")
    g4 = g4.join(last_end_date, g4["profile_id"]==last_end_date["l_profile_id"], "left").drop("l_profile_id")
    g4 = g4.where("endDate = last_end_date").drop("last_end_date")

    final = g1.unionByName(g3).unionByName(g4)
    
    pp_df = final.select("profile_id", "title", "startDate")


    ndf = custom_join(profile_df, pp_df, "profile", "pp", profile_df["id"] == pp_df["profile_id"], "left")
    ndf = ndf.drop("profile_id")
    ndf = ndf.withColumnRenamed("startDate", "last_mission_start_date")
    ndf = ndf.withColumnRenamed("title", "last_mission_title")

    return ndf




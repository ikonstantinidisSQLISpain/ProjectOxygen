import re
from typing import Mapping
import json
from pathlib import Path


_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(identifier: str) -> str:
    """
    Quote a SQL identifier using backticks.

    Backticks inside the identifier are escaped according to Spark SQL rules.
    """
    if not isinstance(identifier, str):
        raise TypeError("SQL identifiers must be strings.")

    identifier = identifier.strip()

    if not identifier:
        raise ValueError("SQL identifiers cannot be empty.")

    return f"`{identifier.replace('`', '``')}`"


def _quote_string(value: str) -> str:
    """
    Quote a string literal for Spark SQL.
    """
    if not isinstance(value, str):
        raise TypeError("SQL string values must be strings.")

    return "'" + value.replace("'", "''") + "'"


def update_table_metadata(
    catalog: str,
    schema: str,
    table: str,
    table_comment: str,
    table_properties: Mapping[str, str],
    column_comments: Mapping[str, str],
) -> None:
    """
    Update a Databricks Unity Catalog table's comment, properties,
    and column comments.

    Args:
        catalog:
            The Unity Catalog catalog name.

        schema:
            The schema name.

        table:
            The table name.

        table_comment:
            The comment to apply to the table.

        table_properties:
            A mapping of table property names to their values.

        column_comments:
            A mapping where keys are column names and values are
            the comments to apply to those columns.

    Raises:
        TypeError:
            If an argument has an invalid type.

        ValueError:
            If a required string is empty or a mapping contains
            invalid keys/values.

    Example:
        update_table_metadata(
            catalog="main",
            schema="sales",
            table="customers",
            table_comment="Contains customer information.",
            table_properties={
                "domain": "sales",
                "data_owner": "data_team",
            },
            column_comments={
                "customer_id": "Unique customer identifier.",
                "email": "Customer email address.",
            },
        )
    """

    # ------------------------------------------------------------------
    # Validate arguments
    # ------------------------------------------------------------------

    for name, value in {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "table_comment": table_comment,
    }.items():
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string.")

        if not value.strip():
            raise ValueError(f"{name} cannot be empty.")

    if not isinstance(table_properties, Mapping):
        raise TypeError("table_properties must be a dictionary or mapping.")

    if not isinstance(column_comments, Mapping):
        raise TypeError("column_comments must be a dictionary or mapping.")

    for key, value in table_properties.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(
                "All table property names must be non-empty strings."
            )

        if not isinstance(value, str):
            raise TypeError(
                f"Table property '{key}' must have a string value."
            )

    for column_name, comment in column_comments.items():
        if not isinstance(column_name, str) or not column_name.strip():
            raise ValueError(
                "All column names must be non-empty strings."
            )

        if not isinstance(comment, str):
            raise TypeError(
                f"Comment for column '{column_name}' must be a string."
            )

    # ------------------------------------------------------------------
    # Build fully qualified table name
    # ------------------------------------------------------------------

    full_table_name = ".".join(
        [
            _quote_identifier(catalog),
            _quote_identifier(schema),
            _quote_identifier(table),
        ]
    )

    # ------------------------------------------------------------------
    # Update table comment
    # ------------------------------------------------------------------

    spark.sql(
        f"""
        COMMENT ON TABLE {full_table_name}
        IS {_quote_string(table_comment)}
        """
    )

    # ------------------------------------------------------------------
    # Update table properties
    # ------------------------------------------------------------------

    if table_properties:
        properties_sql = ", ".join(
            f"{_quote_string(key)} = {_quote_string(value)}"
            for key, value in table_properties.items()
        )

        spark.sql(
            f"""
            ALTER TABLE {full_table_name}
            SET TBLPROPERTIES ({properties_sql})
            """
        )

    # ------------------------------------------------------------------
    # Update column comments
    # ------------------------------------------------------------------

    for column_name, comment in column_comments.items():
        quoted_column_name = _quote_identifier(column_name)

        spark.sql(
            f"""
            ALTER TABLE {full_table_name}
            ALTER COLUMN {quoted_column_name}
            COMMENT {_quote_string(comment)}
            """
        )

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






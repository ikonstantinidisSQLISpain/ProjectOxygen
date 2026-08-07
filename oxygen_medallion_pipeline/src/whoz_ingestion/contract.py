"""Table schemas as data: YAML in, the DDL string create_streaming_table(schema=) takes out.

The columns are the data; the DDL string is a rendering of them. Written the other way
round — as a hand-quoted SQL string — a bare apostrophe inside a COMMENT ends the string
literal early and takes the whole schema down with it, and neither py_compile nor ruff nor
`databricks bundle validate` notices. Escaping here means it is done once, by code, rather
than remembered at every column.

Read by BOTH the pipeline (transformations/silver/*.py, via shaping/) and the test suite,
from the same files on the same import path — see resources/whoz_ingestion_etl.pipeline.yml's
root_path and pyproject.toml's pythonpath.

    schemas/<entity>.yml   the columns, in order, as prose comments YAML quotes for you
    load_columns(entity)   -> [{"name", "type", "comment"?}, ...]
    ddl(columns)           -> the SQL string, apostrophes escaped

Validation is here, not in a test, so a malformed schema fails the same way in a pipeline
update and in pytest — and fails at import, before anything has a chance to run on a
half-built schema.
"""

from pathlib import Path

import yaml

SCHEMA_DIR = Path(__file__).parent / "schemas"

# One column: {"name": ..., "type": ..., "comment": ...}, comment optional. A dict rather
# than a NamedTuple because it is what yaml.safe_load already hands back, and nothing here
# needs behaviour — only the shape.
Column = dict[str, str]

# The only keys a column entry may carry. Enforced rather than ignored: a typo'd `commnet:`
# would otherwise drop the comment silently, which is the same class of invisible loss this
# module exists to remove.
COLUMN_KEYS = frozenset({"name", "type", "comment"})

# AUTO CDC's SCD2 validity window, appended to a base column list to describe the history
# table. Both must be TIMESTAMP to match every sequence_by in this project
# (source_last_modified_at) — confirmed against the docs, not guessed; get it wrong and the
# SCD2 flow fails to attach at update time, which `databricks bundle validate` does not catch.
SCD2_COLUMNS: list[Column] = [
    {
        "name": "__START_AT",
        "type": "TIMESTAMP",
        "comment": "Start of this version's validity window (SCD2, added by AUTO CDC)",
    },
    {
        "name": "__END_AT",
        "type": "TIMESTAMP",
        "comment": "End of this version's validity window; NULL means still current (SCD2, added by AUTO CDC)",
    },
]


def load_columns(entity: str) -> list[Column]:
    """Read schemas/<entity>.yml into an ordered list of column definitions.

    Order is preserved and is part of the contract — AUTO CDC matches source to target
    positionally as well as by name.
    """
    path = SCHEMA_DIR / f"{entity}.yml"
    if not path.exists():
        available = sorted(p.stem for p in SCHEMA_DIR.glob("*.yml"))
        raise FileNotFoundError(f"no schema file for entity {entity!r} at {path}. Available: {available}")

    # Loaded from an open file rather than from read_text(): on a syntax error PyYAML reports
    # the stream's name, so the message points at the offending file instead of at
    # `"<unicode string>", line 138`. Eight YAML files now across schemas/ and checks/, so that matters.
    with path.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    columns = (document or {}).get("columns") if isinstance(document, dict) else None
    if not isinstance(columns, list) or not columns:
        raise ValueError(f"{path} must hold a non-empty 'columns:' list of column definitions")

    for position, column in enumerate(columns):
        where = f"{path}, column {position} ({column.get('name', '?') if isinstance(column, dict) else column!r})"
        if not isinstance(column, dict):
            raise TypeError(f"{where}: each column must be a mapping with name/type/comment keys")
        unknown = sorted(set(column) - COLUMN_KEYS)
        if unknown:
            raise ValueError(f"{where}: unknown key(s) {unknown}. Allowed: {sorted(COLUMN_KEYS)}")
        for required in ("name", "type"):
            value = column.get(required)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{where}: '{required}' is required and must be a non-empty string")

    return columns


def ddl(columns: list[Column]) -> str:
    """Render column definitions as the DDL string create_streaming_table(schema=) takes.

    The escaping below is the whole reason this function exists: it is the one place in the
    project that has to remember it. Two characters need it inside a single-quoted Spark SQL
    literal, and both fail silently rather than loudly:

        '   ends the literal early and takes the rest of the schema with it
        \\   is an escape character, so "C:\\Users\\x" arrives as "C:Usersx" and a comment
            containing "\\b" arrives with a backspace in it — measured, not assumed

    No comment carries a backslash today. Both are handled anyway, because "handled once, in
    code" is the property being bought here, and a future comment holding a path or a regex
    should not reintroduce the class of failure this module exists to remove. Backslash first:
    doing it second would re-escape the ones the apostrophe rule just added.
    """
    clauses = []
    for column in columns:
        clause = f"{column['name']} {column['type']}"
        comment = column.get("comment") or ""
        if comment:
            escaped = comment.replace("\\", "\\\\").replace("'", "''")
            clause += " COMMENT '" + escaped + "'"
        clauses.append(clause)
    return ",\n    ".join(clauses)

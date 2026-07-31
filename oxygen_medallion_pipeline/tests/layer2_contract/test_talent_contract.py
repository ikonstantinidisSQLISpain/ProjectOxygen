"""Layer 2 — does the declared schema still describe what the code produces? (talent entity)

TALENT_COLUMNS (utilities/shaping/talent.py) is the DDL string handed to
create_streaming_table(schema=...) for silver.whoz_talents and, with __START_AT/__END_AT
appended, silver.whoz_talent_versions.

Nothing else in the toolchain looks inside that string. It is an opaque blob to
py_compile, to ruff, to pytest and to `databricks bundle validate` — all four pass green
on a schema that cannot parse at all. The only other thing that would notice is a live
pipeline update. These tests close that gap for the price of one local SparkSession.

Extending to another table: give it a DDL constant next to its shaping function, then add
the same two tests. The helpers do the work — see tests/helpers.py.

The three tests below are deliberately terse. They were copied from the profile block in
test_profile_contract.py, and the long explanatory comments that say *why* each assertion
is written the way it is live there.
"""

from helpers import assert_schema_matches_ddl, ddl_columns
from utilities.shaping.talent import TALENT_COLUMNS, TALENT_HISTORY_COLUMNS, shape_talent


# -------------------------------------------------------------------------------------
# The talent entity. Two tests, the same two questions — this is the whole cost of adding
# an entity to layer 2.
# -------------------------------------------------------------------------------------
def test_talent_columns_is_valid_ddl():
    assert len(ddl_columns(TALENT_COLUMNS)) > 0


def test_declared_schema_matches_shape_talent_output(talent_fixture):
    result = shape_talent(talent_fixture("hazards"))

    assert_schema_matches_ddl(result, TALENT_COLUMNS)


def test_talent_history_schema_adds_only_the_scd2_columns():
    base = ddl_columns(TALENT_COLUMNS)
    history = ddl_columns(TALENT_HISTORY_COLUMNS)

    assert history[: len(base)] == base, "the SCD2 columns must be appended, not interleaved"
    assert history[len(base) :] == [("__START_AT", "timestamp"), ("__END_AT", "timestamp")]
    assert dict(base)["source_last_modified_at"] == "timestamp", (
        "sequence_by column and the SCD2 window columns must share a type"
    )

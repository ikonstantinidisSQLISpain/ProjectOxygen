"""Layer 2 — does the declared schema still describe what the code produces?

PROFILE_COLUMNS (utilities/profile_shaping.py) is the DDL string handed to
create_streaming_table(schema=...) for silver.whoz_profiles and, with __START_AT/__END_AT
appended, silver.whoz_profile_history.

Nothing else in the toolchain looks inside that string. It is an opaque blob to
py_compile, to ruff, to pytest and to `databricks bundle validate` — all four pass green
on a schema that cannot parse at all. The only other thing that would notice is a live
pipeline update. These tests close that gap for the price of one local SparkSession.

Extending to another table: give it a DDL constant next to its shaping function, then add
the same two tests. The helpers do the work — see tests/helpers.py.
"""

from helpers import assert_schema_matches_ddl, ddl_columns
from utilities.profile_shaping import PROFILE_COLUMNS, PROFILE_HISTORY_COLUMNS, shape_profile
from utilities.talent_shaping import TALENT_COLUMNS, TALENT_HISTORY_COLUMNS, shape_talent


def test_profile_columns_is_valid_ddl():
    # Catches the easy and genuinely likely mistake: an unescaped apostrophe inside a
    # COMMENT (write 'it''s', not 'it's') silently ends the string literal early and takes
    # the whole schema down with it. StructType.fromDDL raises on that; nothing else does.
    parsed = ddl_columns(PROFILE_COLUMNS)

    assert len(parsed) > 0


def test_declared_schema_matches_shape_profile_output(profile_fixture):
    # The contract only means something if it describes what shape_profile() actually
    # emits. Add a column to one and not the other, or change a type on one side, and this
    # fails here in seconds instead of part-way through a pipeline update.
    #
    # Uses the sparse fixture on purpose: every column NULL still carries a type, so this
    # compares the declared schema against shaping at its least informative, which is
    # where an accidental type change is most likely to slip through.
    result = shape_profile(profile_fixture("hazards"))

    assert_schema_matches_ddl(result, PROFILE_COLUMNS)


def test_history_schema_adds_only_the_scd2_columns():
    # silver.whoz_profile_history is PROFILE_COLUMNS + __START_AT/__END_AT, and AUTO CDC
    # requires both to be typed to match sequence_by (source_last_modified_at, TIMESTAMP).
    # Get that wrong and the SCD2 flow fails to attach at update time, not at validate
    # time. Note this parses PROFILE_HISTORY_COLUMNS — the actual string
    # silver_whoz_profile.py passes to create_streaming_table, not a copy of it.
    base = ddl_columns(PROFILE_COLUMNS)
    history = ddl_columns(PROFILE_HISTORY_COLUMNS)

    assert history[: len(base)] == base, "the SCD2 columns must be appended, not interleaved"
    assert history[len(base) :] == [("__START_AT", "timestamp"), ("__END_AT", "timestamp")]
    sequence_by_type = dict(base)["source_last_modified_at"]
    assert sequence_by_type == "timestamp", (
        "sequence_by column and the SCD2 window columns must share a type"
    )


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

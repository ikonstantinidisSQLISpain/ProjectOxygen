"""Layer 2 — does the declared schema still describe what the code produces? (user entity)

USER_COLUMNS (whoz_ingestion/shaping/user.py) is the DDL string handed to
create_streaming_table(schema=...) for silver.whoz_users and, with __START_AT/__END_AT
appended, silver.whoz_user_versions.

Copied from the talent pair; the long explanatory comments about why each assertion is
written the way it is live in test_profile_contract.py.
"""

from helpers import assert_schema_matches_ddl, ddl_columns

from whoz_ingestion.shaping.user import USER_COLUMNS, USER_HISTORY_COLUMNS, shape_user


def test_user_columns_is_valid_ddl():
    assert len(ddl_columns(USER_COLUMNS)) > 0


def test_declared_schema_matches_shape_user_output(user_fixture):
    # The hazards fixture on purpose: it holds the sparse record, where almost every column is
    # NULL but still carries a type, so this compares the DDL against shaping at its least
    # informative — which is where a wrong cast hides.
    result = shape_user(user_fixture("hazards"))

    assert_schema_matches_ddl(result, USER_COLUMNS)


def test_user_history_schema_adds_only_the_scd2_columns():
    base = ddl_columns(USER_COLUMNS)
    history = ddl_columns(USER_HISTORY_COLUMNS)

    assert history[: len(base)] == base, "the SCD2 columns must be appended, not interleaved"
    assert history[len(base) :] == [("__START_AT", "timestamp"), ("__END_AT", "timestamp")]
    assert dict(base)["source_last_modified_at"] == "timestamp", (
        "sequence_by column and the SCD2 window columns must share a type"
    )

"""Layer 3 — are the data quality checks themselves right? (user behaviour)

The behavioural half of layer 3, for the user entity: the checks pass clean data and fire on
the records built to break them. Only a human can write this half — hygiene proves a check is
well-formed, never that it means what it is supposed to mean. The cross-cutting half is
test_rule_hygiene.py, and README.md#data-quality-checks has the reasoning for both.
"""

import pytest
from helpers import assert_dqx_violations, assert_no_dqx_violations, assert_no_skipped_checks, dqx_failing_counts

from whoz_ingestion.checks import CHECKS
from whoz_ingestion.shaping.user import shape_user

USER_CHECKS = CHECKS["whoz_user_shaped"]


@pytest.fixture()
def checked(dq_engine, user_fixture):
    """fixtures/whoz_users/<name>.json -> shaped, with _errors and _warnings appended."""
    return lambda name: dq_engine.apply_checks_by_metadata(shape_user(user_fixture(name)), USER_CHECKS)


@pytest.mark.parametrize("fixture_name", ["typical", "hazards"])
def test_user_rules_pass_on_valid_records(checked, fixture_name):
    # hazards.json is the one that matters here. Every record in it is awkward — an empty
    # collection where others have a map, an absent key where others have a value, second
    # rather than millisecond timestamps, the literal string "null" as an id — and every one
    # of them is legitimate. A check firing on this file is a false positive, which is worse
    # than no check because it teaches people to ignore the quarantine table.
    assert_no_dqx_violations(checked(fixture_name), USER_CHECKS)


def test_user_rules_catch_the_records_designed_to_break_them(checked):
    assert_dqx_violations(
        checked("violations"),
        USER_CHECKS,
        {
            "user_id_not_null": 1,
            "last_modified_at_not_null": 1,
            "federation_count_at_most_one": 1,
            "agentic_studio_roles_still_empty": 1,
            "user_not_removed": 1,
            "user_last_connection_not_before_created": 1,
        },
    )


def test_every_user_rule_is_covered_by_the_violations_fixture(checked):
    result = checked("violations")
    assert_no_skipped_checks(result)

    uncovered = [name for name, count in dqx_failing_counts(result, USER_CHECKS).items() if count == 0]

    assert not uncovered, (
        f"these checks never fire on fixtures/whoz_users/violations.json, so nothing "
        f"proves they work: {uncovered}. Add a record that violates each."
    )

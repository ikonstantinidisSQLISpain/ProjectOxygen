"""Layer 3 — are the data quality checks themselves right? (talent behaviour)

The behavioural half of layer 3, for the talent entity: the checks pass clean data and fire
on the records built to break them. Only a human can write this half — hygiene proves a check
is well-formed, never that it means what it is supposed to mean. The cross-cutting half is
test_rule_hygiene.py, and README.md#data-quality-checks has the reasoning for both.
"""

import pytest
from helpers import assert_dqx_violations, assert_no_dqx_violations, assert_no_skipped_checks, dqx_failing_counts

from whoz_ingestion.checks import CHECKS
from whoz_ingestion.shaping.talent import shape_talent

# -------------------------------------------------------------------------------------
# Behaviour — the talent checks, resolved against real shape_talent() output.
#
# Structurally identical to the profile block in test_profile_rules.py, on purpose: this is
# what adding an entity to layer 3 costs. Three tests, one fixture folder, no changes to the
# hygiene tests.
# -------------------------------------------------------------------------------------
TALENT_CHECKS = CHECKS["whoz_talent_shaped"]


@pytest.fixture()
def checked(dq_engine, talent_fixture):
    """fixtures/whoz_talents/<name>.json -> shaped, with _errors and _warnings appended."""
    return lambda name: dq_engine.apply_checks_by_metadata(shape_talent(talent_fixture(name)), TALENT_CHECKS)


@pytest.mark.parametrize("fixture_name", ["typical", "hazards"])
def test_talent_rules_pass_on_valid_records(checked, fixture_name):
    assert_no_dqx_violations(checked(fixture_name), TALENT_CHECKS)


def test_talent_rules_catch_the_records_designed_to_break_them(checked):
    assert_dqx_violations(
        checked("violations"),
        TALENT_CHECKS,
        {
            "talent_pk_not_null": 1,
            # Two records, and that is the point rather than sloppiness: the array-form
            # record trips this as well as profile_is_not_an_array, because NULLing every
            # profile_* column is exactly the symptom the array form produces. Cause and
            # symptom both firing on the same row is what makes the pair worth having.
            "talent_has_a_profile": 2,
            "profile_is_not_an_array": 1,
            "embedded_talent_id_agrees": 1,
            "profile_completion_rate_is_fraction": 1,
            "last_connection_not_before_created": 1,
        },
    )


def test_every_talent_rule_is_covered_by_the_violations_fixture(checked):
    result = checked("violations")
    assert_no_skipped_checks(result)

    uncovered = [name for name, count in dqx_failing_counts(result, TALENT_CHECKS).items() if count == 0]

    assert not uncovered, (
        f"these checks never fire on fixtures/whoz_talents/violations.json, so nothing "
        f"proves they work: {uncovered}. Add a record that violates each."
    )

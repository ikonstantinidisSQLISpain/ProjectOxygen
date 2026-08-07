"""Layer 3 — are the data quality checks themselves right? (profile behaviour)

The behavioural half of layer 3, for the profile entity: the checks pass clean data and fire
on the records built to break them. Only a human can write this half — hygiene proves a check
is well-formed, never that it means what it is supposed to mean. The cross-cutting half is
test_rule_hygiene.py, and README.md#data-quality-checks has the reasoning for both.

The checks are applied here exactly as transformations/silver/whoz_profile.py applies them —
`dq.apply_checks_by_metadata(shape_profile(...), CHECKS["whoz_profile_shaped"])` — so what is
under test is the real check list against real shaped output, not a paraphrase of either.
"""

import pytest
from helpers import assert_dqx_violations, assert_no_dqx_violations, assert_no_skipped_checks, dqx_failing_counts

from whoz_ingestion.checks import CHECKS
from whoz_ingestion.shaping.profile import shape_profile

# -------------------------------------------------------------------------------------
# Behaviour — the profile checks, resolved against real shape_profile() output.
# -------------------------------------------------------------------------------------
PROFILE_CHECKS = CHECKS["whoz_profile_shaped"]


@pytest.fixture()
def checked(dq_engine, profile_fixture):
    """fixtures/whoz_profiles/<name>.json -> shaped, with _errors and _warnings appended."""
    return lambda name: dq_engine.apply_checks_by_metadata(shape_profile(profile_fixture(name)), PROFILE_CHECKS)


@pytest.mark.parametrize("fixture_name", ["typical", "hazards"])
def test_profile_rules_pass_on_valid_records(checked, fixture_name):
    # Both files are records the pipeline should accept without complaint — typical.json
    # is ordinary data, hazards.json is awkward but legitimate. A check that fires here is
    # a false positive, and a false positive in a quality check is worse than no check: it
    # trains everyone to ignore the quarantine table.
    assert_no_dqx_violations(checked(fixture_name), PROFILE_CHECKS)


def test_profile_rules_catch_the_records_designed_to_break_them(checked):
    # The other half: every check must actually fire on something. violations.json holds
    # one record per check, so the expected counts are all 1 — and assert_dqx_violations
    # treats this dict as complete, so a check that fires on a record it wasn't meant to
    # catch fails here too.
    assert_dqx_violations(
        checked("violations"),
        PROFILE_CHECKS,
        {
            "profile_id_not_null": 1,
            "talent_id_not_null": 1,
            "is_main_version": 1,
            "completion_rate_is_fraction": 1,
        },
    )


def test_every_profile_rule_is_covered_by_the_violations_fixture(checked):
    # The guard on the test above: it asserts the counts it lists, but adding a check to
    # whoz_ingestion/checks/whoz_profile_shaped.yml without adding a record that trips it
    # would leave the new check silently unproven. This fails until the fixture covers it.
    result = checked("violations")
    assert_no_skipped_checks(result)

    uncovered = [name for name, count in dqx_failing_counts(result, PROFILE_CHECKS).items() if count == 0]

    assert not uncovered, (
        f"these checks never fire on fixtures/whoz_profiles/violations.json, so nothing "
        f"proves they work: {uncovered}. Add a record that violates each."
    )


def test_error_criticality_withholds_a_row_and_warn_does_not(dq_engine, checked):
    # Pins the contract the whole pipeline shape rests on, because it is the one thing that
    # changed when the expectation decorators went away and nothing else asserts it.
    #
    #   error -> the row is NOT in get_valid (so not in silver.whoz_profiles) and IS in
    #            get_invalid (so it is in the quarantine table)
    #   warn  -> the row is in BOTH: it flows into silver as normal and is also quarantined,
    #            carrying the _warnings struct that says why
    #
    # violations.json has four records: one null profile_id (error) and three that each trip
    # exactly one warn check. So valid drops one row and quarantine keeps all four — which is
    # also the concrete answer to "is quarantine a dead-letter queue?". It is not.
    result = checked("violations")
    # Every behaviour test goes through this, including one that counts rows rather than
    # violations: a skipped check would put every row in quarantine and the counts below
    # would be wrong for a reason that has nothing to do with criticality.
    assert_no_skipped_checks(result)

    valid = dq_engine.get_valid(result)
    invalid = dq_engine.get_invalid(result)

    assert result.count() == 4
    assert valid.count() == 3, "only the null-profile_id row should be withheld from silver"
    assert invalid.count() == 4, "every flagged row is quarantined, warn-level ones included"
    assert [row["profile_id"] for row in valid.select("profile_id").collect()].count(None) == 0

    # get_valid drops the result columns, which is what keeps the shaped view schema-identical
    # to what create_auto_cdc_flow already expects. get_invalid keeps them, which is what makes
    # the quarantine table worth reading.
    assert "_errors" not in valid.columns and "_warnings" not in valid.columns
    assert "_errors" in invalid.columns and "_warnings" in invalid.columns

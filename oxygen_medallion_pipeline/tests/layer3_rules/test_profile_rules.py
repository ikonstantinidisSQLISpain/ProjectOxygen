"""Layer 3 — are the data quality rules themselves right? (profile behaviour)

The pipeline's quality rules live in utilities/expectations.py as {name: SQL} dicts and
are applied by the @dp.expect_all* decorators in transformations/. Those predicates are
strings: a rule naming a column that does not exist, or one that matches nothing it was
meant to catch, is invisible to py_compile, ruff and `databricks bundle validate` alike.
Worse than a broken test, a broken *rule* fails silently — the pipeline reports 100% pass
and nobody looks again.

So there are two kinds of test in this layer, and both matter:

  1. Hygiene, over every rule in the project via ALL_RULE_SETS. Automatic: add a rule to
     utilities/expectations.py and these start covering it with no edit here. That half
     lives in test_rule_hygiene.py.
  2. Behaviour, per rule set: it passes clean data (fixtures/whoz_profiles/typical.json,
     hazards.json) and it catches the rows it is meant to catch
     (fixtures/whoz_profiles/violations.json). This is the half that has to be written by
     hand, because only you know what a rule is supposed to mean.

This module is (2) for the profile entity; test_talent_rules.py is the same for talents.
"""

import pytest
from helpers import assert_no_violations, assert_violations, failing_counts
from utilities.expectations import PROFILE_MUST_HOLD, PROFILE_SHOULD_HOLD
from utilities.shaping.profile import shape_profile

# -------------------------------------------------------------------------------------
# Behaviour — the profile rules, resolved against real shape_profile() output.
# -------------------------------------------------------------------------------------
PROFILE_RULES = {**PROFILE_MUST_HOLD, **PROFILE_SHOULD_HOLD}


@pytest.mark.parametrize("fixture_name", ["typical", "hazards"])
def test_profile_rules_pass_on_valid_records(profile_fixture, fixture_name):
    # Both files are records the pipeline should accept without complaint — typical.json
    # is ordinary data, hazards.json is awkward but legitimate. A rule that fires here is
    # a false positive, and a false positive in a quality rule is worse than no rule: it
    # trains everyone to ignore the dashboard.
    shaped = shape_profile(profile_fixture(fixture_name))

    assert_no_violations(shaped, PROFILE_RULES)


def test_profile_rules_catch_the_records_designed_to_break_them(profile_fixture):
    # The other half: every rule must actually fire on something. violations.json holds
    # one record per rule, so the expected counts are all 1 — and assert_violations treats
    # this dict as complete, so a rule that fires on a record it wasn't meant to catch
    # fails here too.
    shaped = shape_profile(profile_fixture("violations"))

    assert_violations(
        shaped,
        PROFILE_RULES,
        {
            "profile_id_not_null": 1,
            "talent_id_not_null": 1,
            "is_main_version": 1,
            "completion_rate_is_fraction": 1,
        },
    )


def test_every_profile_rule_is_covered_by_the_violations_fixture(profile_fixture):
    # The guard on the test above: it asserts the counts it lists, but adding a rule to
    # utilities/expectations.py without adding a record that trips it would leave the new
    # rule silently unproven. This fails until the fixture covers it.
    shaped = shape_profile(profile_fixture("violations"))

    uncovered = [name for name, count in failing_counts(shaped, PROFILE_RULES).items() if count == 0]

    assert not uncovered, (
        f"these rules never fire on fixtures/whoz_profiles/violations.json, so nothing "
        f"proves they work: {uncovered}. Add a record that violates each."
    )

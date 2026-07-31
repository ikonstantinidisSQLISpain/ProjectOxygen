"""Layer 3 — are the data quality rules themselves right? (talent behaviour)

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
  2. Behaviour, per rule set: it passes clean data (fixtures/whoz_talents/typical.json,
     hazards.json) and it catches the rows it is meant to catch
     (fixtures/whoz_talents/violations.json). This is the half that has to be written by
     hand, because only you know what a rule is supposed to mean.

This module is (2) for the talent entity; test_profile_rules.py is the same for profiles.
"""

import pytest
from helpers import assert_no_violations, assert_violations, failing_counts
from utilities.expectations import TALENT_MUST_HOLD, TALENT_SHOULD_HOLD
from utilities.shaping.talent import shape_talent

# -------------------------------------------------------------------------------------
# Behaviour — the talent rules, resolved against real shape_talent() output.
#
# Structurally identical to the profile block in test_profile_rules.py, on purpose: this is
# what adding an entity to layer 3 costs. Three tests, one fixture folder, no changes to the
# hygiene tests.
# -------------------------------------------------------------------------------------
TALENT_RULES = {**TALENT_MUST_HOLD, **TALENT_SHOULD_HOLD}


@pytest.mark.parametrize("fixture_name", ["typical", "hazards"])
def test_talent_rules_pass_on_valid_records(talent_fixture, fixture_name):
    shaped = shape_talent(talent_fixture(fixture_name))

    assert_no_violations(shaped, TALENT_RULES)


def test_talent_rules_catch_the_records_designed_to_break_them(talent_fixture):
    shaped = shape_talent(talent_fixture("violations"))

    assert_violations(
        shaped,
        TALENT_RULES,
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


def test_every_talent_rule_is_covered_by_the_violations_fixture(talent_fixture):
    shaped = shape_talent(talent_fixture("violations"))

    uncovered = [name for name, count in failing_counts(shaped, TALENT_RULES).items() if count == 0]

    assert not uncovered, (
        f"these rules never fire on fixtures/whoz_talents/violations.json, so nothing "
        f"proves they work: {uncovered}. Add a record that violates each."
    )

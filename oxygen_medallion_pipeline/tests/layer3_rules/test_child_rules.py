"""Layer 3 — are the data quality checks themselves right? (child-table behaviour)

The behavioural half for the datasets exploded out of a payload array — half of every check
in the project, and until those queries moved into whoz_ingestion/shaping/, the half nothing
could check. Their SQL was inline in transformations/, unreachable from pytest, so their
predicates were only ever handed to F.expr(), which parses a predicate but resolves no column
names: a rule naming `proficiency_level` where the column is `proficiency` passed the whole
suite.

Running the real query against a bronze fixture is what recovers that — and under DQX it
matters more, not less. DQX does not raise on a column it cannot resolve: it marks the check
`skipped=true` and carries on, which at `error` criticality quarantines every row and under
`suppress_skipped` checks nothing at all. `helpers.assert_no_skipped_checks` is what turns
that back into a loud local failure, and every assertion below runs through it.

The cross-cutting half is test_rule_hygiene.py; README.md#data-quality-checks has the
reasoning for both.
"""

import pytest
from helpers import (
    assert_dqx_violations,
    assert_no_dqx_violations,
    assert_no_skipped_checks,
    dqx_failing_counts,
)

from whoz_ingestion.checks import CHECKS
from whoz_ingestion.shaping.profile_aptitudes import aptitudes_sql
from whoz_ingestion.shaping.profile_position_aptitude_refs import aptitude_refs_sql
from whoz_ingestion.shaping.profile_positions import positions_sql
from whoz_ingestion.shaping.talent_workspace_history import workspace_history_sql
from whoz_ingestion.shaping.user_workspace_roles import workspace_roles_sql

# Which query feeds which dataset, split by whose fixtures it reads. The dataset key is the
# same one CHECKS and the @dp.table function in transformations/ use.
#
# One module per query as of the split, so the import list above is also the inventory: a new
# exploded dataset is a new shaping module, a new import here, and a new entry below.
PROFILE_CHILDREN = {
    "whoz_profile_aptitudes": aptitudes_sql,
    "whoz_profile_positions": positions_sql,
    "whoz_position_aptitude_refs": aptitude_refs_sql,
}
TALENT_CHILDREN = {
    "whoz_talent_workspace_history": workspace_history_sql,
}
USER_CHILDREN = {
    "whoz_user_workspace_roles": workspace_roles_sql,
}

# (dataset, fixture) pairs that legitimately yield no rows, because the fixture carries nothing
# for that collection: hazards.json's one position has "aptitudeReferences": [], so the bridge
# query explodes to nothing. typical.json is what exercises that dataset's checks against data.
#
# Listed explicitly rather than tolerated silently, because an empty result makes every DQX
# assertion vacuous — assert_no_skipped_checks now rejects one outright, and the reason is in
# its docstring: DQX reports `skipped` per row, so with no rows a typo'd column name is
# invisible. Measured, and the opposite of what the comment here used to claim.
#
# The entry is asserted in both directions below, so this cannot rot: if the fixture ever gains
# an aptitudeReference, the pair stops being empty and the test tells you to delete it from here
# and let the real assertions run.
KNOWN_EMPTY = {("whoz_position_aptitude_refs", "hazards")}


@pytest.mark.parametrize("dataset", sorted(PROFILE_CHILDREN))
@pytest.mark.parametrize("fixture_name", ["typical", "hazards"])
def test_profile_child_rules_pass_on_valid_records(dq_engine, child_query, profile_fixture, dataset, fixture_name):
    # Both files are records the pipeline should accept without complaint. A check that fires
    # here is a false positive, and a false positive in a quality check is worse than no check.
    rows = child_query(PROFILE_CHILDREN[dataset], profile_fixture(fixture_name))

    if (dataset, fixture_name) in KNOWN_EMPTY:
        # All this pair can prove is that the query analyses and every column it names
        # resolves — worth running, but it is not a check of the checks, and routing it
        # through assert_no_dqx_violations would dress it up as one.
        assert rows.isEmpty(), (
            f"{dataset} now produces rows from the {fixture_name} fixture. Remove it from "
            f"KNOWN_EMPTY so the real DQX assertions run against them."
        )
        return

    result = dq_engine.apply_checks_by_metadata(rows, CHECKS[dataset])

    assert_no_dqx_violations(result, CHECKS[dataset])


@pytest.mark.parametrize("dataset", sorted(TALENT_CHILDREN))
@pytest.mark.parametrize("fixture_name", ["typical", "hazards"])
def test_talent_child_rules_pass_on_valid_records(dq_engine, child_query, talent_fixture, dataset, fixture_name):
    result = dq_engine.apply_checks_by_metadata(
        child_query(TALENT_CHILDREN[dataset], talent_fixture(fixture_name)), CHECKS[dataset]
    )

    assert_no_dqx_violations(result, CHECKS[dataset])


# -------------------------------------------------------------------------------------
# The user entity's child table — and the first one with a violations half.
#
# Every other child dataset here is only ever shown clean data: the profile and talent
# violations fixtures trip parent checks, not child ones, so ten of the project's checks are
# applied but never made to FIRE. That gap is documented in the README. The user fixtures close
# it for this dataset, and the three tests below are the shape the others should grow into.
# -------------------------------------------------------------------------------------
@pytest.mark.parametrize("dataset", sorted(USER_CHILDREN))
@pytest.mark.parametrize("fixture_name", ["typical", "hazards"])
def test_user_child_rules_pass_on_valid_records(dq_engine, child_query, user_fixture, dataset, fixture_name):
    result = dq_engine.apply_checks_by_metadata(
        child_query(USER_CHILDREN[dataset], user_fixture(fixture_name)), CHECKS[dataset]
    )

    assert_no_dqx_violations(result, CHECKS[dataset])


def test_user_child_rules_catch_the_records_designed_to_break_them(dq_engine, child_query, user_fixture):
    checks = CHECKS["whoz_user_workspace_roles"]
    result = dq_engine.apply_checks_by_metadata(child_query(workspace_roles_sql, user_fixture("violations")), checks)

    assert_dqx_violations(
        result,
        checks,
        {
            # From violation-null-id, which carries one membership. That record exists for the
            # parent's user_id_not_null check; its child rows inherit the null user_id, so it
            # covers both. Deliberate reuse — a null workspace_id cannot be constructed at all,
            # since that half of the key is a JSON object key.
            "workspace_role_keys_not_null": 1,
            "workspace_role_is_known": 1,
            "embedded_workspace_id_agrees": 1,
        },
    )


def test_every_user_child_rule_is_covered_by_the_violations_fixture(dq_engine, child_query, user_fixture):
    checks = CHECKS["whoz_user_workspace_roles"]
    result = dq_engine.apply_checks_by_metadata(child_query(workspace_roles_sql, user_fixture("violations")), checks)
    assert_no_skipped_checks(result)

    uncovered = [name for name, count in dqx_failing_counts(result, checks).items() if count == 0]

    assert not uncovered, (
        f"these checks never fire on fixtures/whoz_users/violations.json, so nothing proves "
        f"they work: {uncovered}. Add a record that violates each."
    )


def test_extended_year_end_date_parses_rather_than_nulling(dq_engine, child_query, profile_fixture):
    # Pins what actually happens, which is not what end_date_parsed was written expecting.
    #
    # fixtures/whoz_profiles/hazards.json's `hazard-extended-year-end-date` carries
    # endDate "+22015-07-31", and docs/whoz_profile_data_model.md §2 records that
    # try_variant_get(..., 'date') returns NULL on it — which is the case end_date_parsed
    # exists to catch. It does not, here: Spark 4.0 accepts the extended-year ISO form and
    # returns the date, so end_date is non-NULL and the check stays quiet. The measurement in
    # the doc was presumably taken on Databricks Runtime, where the behaviour may still
    # differ; this is the local truth, and the check is therefore unproven by any fixture.
    #
    # Left as an observation rather than "fixed" by inventing a fixture record: a check with
    # nothing that trips it is worth knowing about, and papering over it with a synthetic
    # violation would hide exactly that.
    positions = child_query(positions_sql, profile_fixture("hazards"))
    # Rendered to a string in Spark, never collected as a DATE: Python's datetime.date tops
    # out at year 9999, so .first() on this column raises "year 22015 is out of range" before
    # any assertion runs. Same class of trap as helpers.to_utc_strings covers for timestamps.
    row = (
        positions.filter("position_id = 'pos-extended-year'")
        .selectExpr("end_date_raw", "cast(end_date as string) AS end_date_text")
        .first()
    )

    assert row["end_date_raw"] == "+22015-07-31"
    assert row["end_date_text"] == "+22015-07-31", (
        "if this ever comes back NULL, end_date_parsed has finally found its case and belongs "
        "in a violations-style assertion instead"
    )

    checks = CHECKS["whoz_profile_positions"]
    result = dq_engine.apply_checks_by_metadata(positions, checks)
    # Not just "end_date_parsed counted 0": a check DQX skipped also counts 0, and the two
    # mean opposite things. This asserts the check ran and found nothing.
    assert_no_skipped_checks(result)
    assert dqx_failing_counts(result, checks)["end_date_parsed"] == 0

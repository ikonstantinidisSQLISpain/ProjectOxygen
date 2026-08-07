"""Layer 3 — are the data quality checks themselves right? (hygiene, over every check)

The cross-cutting half of layer 3: every check in whoz_ingestion/checks/*.yml is examined
here for well-formedness, naming and criticality policy, automatically, with no edit to this
file when a check is added. The behavioural half — does a check mean what it is supposed to
mean — is per entity, in test_profile_rules.py and test_talent_rules.py (and, for the child
datasets, test_child_rules.py). README.md#data-quality-checks has the reasoning for both.

What this file can and cannot see is worth stating plainly, because DQX moved the line, and
the boundary was measured against the installed DQX rather than taken from its docs:

  * `DQEngine.validate_checks` — run by the loader at import, no Spark and no client — catches
    an unknown function name, an unexpected argument name, a non-dict `check:`, a missing
    `function:` and a check with no target at all. Asserting any of those here would restate a
    raise that already happened, so this file does not.
  * It does NOT look at `name` (a check with none validates clean) and does NOT mind a missing
    `criticality:`, where DQX then defaults to `error` — silently promoting a warning into a
    row withheld from silver. Those are the loader's guarantees and this file's assertions.
  * Neither catches a column that does not exist: DQX resolves columns at apply time and
    *skips* a check it cannot resolve rather than failing. Only the behaviour files, through
    `helpers.assert_no_skipped_checks`, can see that — and only on a non-empty DataFrame.
"""

import pathlib
import re

import pytest
from helpers import assert_no_skipped_checks

from whoz_ingestion import checks as checks_module
from whoz_ingestion.checks import CHECKS

# Every (dataset, check) in the project, flattened for parametrization so each check shows up
# as its own named test case — the test id is what names the offending check in a failure.
ALL_CHECKS = [
    pytest.param(dataset, check, id=f"{dataset}.{check.get('name', '?')}")
    for dataset, checks in CHECKS.items()
    for check in checks
]

TRANSFORMATIONS = pathlib.Path(__file__).parent.parent.parent / "src" / "whoz_ingestion_etl" / "transformations"

# The two criticalities DQX defines, and the only two this project uses. `error` withholds the
# row from the dataset and sends it to quarantine; `warn` keeps it in both.
CRITICALITIES = ("error", "warn")

# The check functions a criticality of `error` may use. See
# test_error_criticality_checks_only_ever_guard_a_key for why this is one entry long.
KEY_CHECK_FUNCTIONS = frozenset({"is_not_null"})


# -------------------------------------------------------------------------------------
# Hygiene — applies to every check in the project, automatically.
# -------------------------------------------------------------------------------------
@pytest.mark.parametrize(("dataset", "check"), ALL_CHECKS)
def test_check_is_well_formed(dataset, check):
    # snake_case names, because the name becomes a metric label and the key of every
    # expected-count dict in the behaviour files — the two places these are read by someone
    # who wasn't looking at the YAML.
    name = check["name"]
    assert re.fullmatch(r"[a-z][a-z0-9_]*", name), f"{dataset}: check names should be snake_case, got {name!r}"

    criticality = check.get("criticality")
    assert criticality in CRITICALITIES, (
        f"{dataset}.{name}: criticality must be one of {list(CRITICALITIES)}, got {criticality!r}. "
        f"DQX defaults to `error` when it is absent, which silently promotes a warning into a "
        f"row that never reaches silver."
    )

    # Only the assertions DQX does not already make. Measured against DQEngine.validate_checks
    # on the installed version rather than assumed — it *does* reject a non-dict `check:`
    # ("'check' field should be a dictionary"), a missing `function:` ("'function' field is
    # missing in the 'check' block") and a check with neither arguments nor for_each_column
    # ("No arguments provided for function ..."), and the loader runs it at import, so
    # asserting those here restated a raise that had already happened. It does NOT inspect
    # `name` at all and does NOT mind a missing `criticality:` — hence the two above.
    body = check["check"]
    arguments = body.get("arguments") or {}

    # sql_expression bypasses DQX's typed messages, so the msg is the only thing a reader
    # gets. Require one, and require it to say more than the predicate already does.
    if body["function"] == "sql_expression":
        message = (arguments.get("msg") or "").strip()
        assert len(message) > 20, (
            f"{dataset}.{name}: a sql_expression check needs a `msg:` explaining what it means "
            f"that this fired. DQX has no typed message to fall back on here."
        )


# -------------------------------------------------------------------------------------
# The loader's own guarantees, tested by feeding it bad input.
#
# These three replace a pair of tests that re-ran the loader's checks over the *already
# loaded* CHECKS — asserting validate_checks passes on a check set whose import had already
# raised if it didn't, and re-deriving name uniqueness that the loader had already enforced.
# Neither could fail: reaching the assertion required the thing it asserted. Their stated
# purpose was to notice if the loader's raise were ever weakened, which is a real thing to
# want and is what is written here instead — the loader is called, on input built to break
# it, and must raise.
# -------------------------------------------------------------------------------------
def _write(directory, stem, body):
    (directory / f"{stem}.yml").write_text(body, encoding="utf-8")


_VALID = """
- name: {name}
  criticality: warn
  check:
    function: is_not_null
    arguments:
      column: some_column
"""


def test_loader_rejects_a_check_dqx_cannot_validate(tmp_path):
    # The import-time DQEngine.validate_checks pass. An unknown function is the cheapest way
    # to trip it; a bad argument name or an invalid criticality goes down the same path.
    _write(tmp_path, "some_dataset", _VALID.format(name="fine"))
    _write(
        tmp_path,
        "broken_dataset",
        "- name: bad\n  criticality: warn\n  check:\n    function: no_such_dqx_function\n"
        "    arguments:\n      column: some_column\n",
    )

    with pytest.raises(ValueError, match="DQX rejected"):
        checks_module._load(tmp_path)


def test_loader_rejects_a_check_with_no_explicit_name(tmp_path):
    # DQX itself never looks at `name` — verified against validate_checks, which returns
    # has_errors=False for a check with no name at all and lets DQX generate one. A generated
    # name is a silently renamed metric and a silently broken expected-count dict, so this
    # guarantee is the loader's alone and nothing else in the toolchain would notice.
    _write(tmp_path, "nameless_dataset", "- criticality: warn\n  check:\n    function: is_not_null\n"
                                         "    arguments:\n      column: some_column\n")

    with pytest.raises(ValueError, match="explicit non-empty `name:`"):
        checks_module._load(tmp_path)


def test_assert_no_skipped_checks_rejects_an_empty_result(spark, dq_engine):
    # Pins the guard that closes this suite's worst blind spot, in the only way that stays
    # true: by exercising it. DQX reports `skipped` per row — `_build_result_struct(...,
    # skipped=True)` is a Column expression — so a result with no rows carries no marker and
    # assert_no_skipped_checks used to return clean from a check it had never evaluated.
    # Measured before the guard existed: mutating position_id to positon_id in
    # whoz_position_aptitude_refs.yml failed on the 2-row `typical` fixture and passed on the
    # 0-row `hazards` one. Without this test, the guard is one refactor away from being lost
    # again, and its absence looks exactly like success.
    checks = [{"name": "some_check", "criticality": "warn", "check": {"function": "is_not_null", "arguments": {"column": "x"}}}]
    rows = spark.createDataFrame([(1,)], "x int")

    # Sanity: the same checks on the same columns, with rows, must pass — otherwise the
    # assertion below would be satisfied by something other than the emptiness.
    assert_no_skipped_checks(dq_engine.apply_checks_by_metadata(rows, checks))

    with pytest.raises(AssertionError, match="no rows"):
        assert_no_skipped_checks(dq_engine.apply_checks_by_metadata(rows.filter("x > 99"), checks))


def test_loader_rejects_a_name_reused_across_two_datasets(tmp_path):
    # Also the loader's alone: DQX validates one file at a time and has no cross-file concept.
    # Two checks sharing a name read as one metric, which is how you debug the wrong table.
    _write(tmp_path, "dataset_one", _VALID.format(name="shared_name"))
    _write(tmp_path, "dataset_two", _VALID.format(name="shared_name"))

    with pytest.raises(ValueError, match="unique across every dataset"):
        checks_module._load(tmp_path)


def test_error_criticality_checks_only_ever_guard_a_key():
    # Policy check, deliberately strict. An `error` check withholds the row: it is not in the
    # silver table, only in quarantine, and the pipeline still reports a healthy run. That is
    # only ever acceptable for a row that cannot be used at all, which in practice means a
    # null primary key — so an `error` check must be a null check, and nothing else.
    #
    # Inspecting the DQX structure rather than searching a SQL string for "IS NOT NULL", which
    # is what this did when the checks were predicates: the function name is the fact now, and
    # it cannot be spelled two ways or hidden inside a longer expression.
    #
    # One loop rather than a parametrized case per check: parametrizing over every check in the
    # project meant most cases returned early and asserted nothing, and the message below
    # already names the offender.
    #
    # If you have a genuine reason to withhold rows on something else, change this test in the
    # same commit — the point is that it takes a deliberate act, not that it is impossible.
    offenders = [
        f"{dataset}.{check['name']}: uses {check['check']['function']}"
        for dataset, checks in CHECKS.items()
        for check in checks
        if check.get("criticality") == "error" and check["check"]["function"] not in KEY_CHECK_FUNCTIONS
    ]

    assert not offenders, (
        "these checks withhold rows from silver on something that is not a null-key check:\n  "
        + "\n  ".join(offenders)
        + f"\nAllowed functions at criticality `error`: {sorted(KEY_CHECK_FUNCTIONS)}. "
        "Move each to `criticality: warn` unless withholding the row is really intended."
    )


# A dataset key being read out of CHECKS anywhere in a transformation, e.g.
#   CHECKS["whoz_talent_shaped"]
# and — separately, and more strictly — one being read out of CHECKS *inside an
# apply_checks_by_metadata call*, which is the only reading of it that enforces anything.
#
# Matched as text rather than by importing transformations/: those modules call dp.table() and
# friends at import time, construct a DQEngine against a real WorkspaceClient, and rely on
# `spark` being injected by Lakeflow, so they cannot be imported outside a running pipeline.
#
# APPLIED is deliberately anchored to the apply call and not to `def <dataset>_checked()`.
# Matching a bare function definition would prove only that a function of that name exists,
# which is not the invariant: a view can be defined and never apply anything, and the checks
# then run on nothing while every test passes. The gap between the call and the CHECKS[...]
# argument is bounded and forbidden from spanning a second apply call, so two adjacent
# datasets cannot satisfy each other's half of the match. If this regex ever stops matching
# the real call style, every dataset reports as unapplied at once — loud and obviously wrong,
# which is the right direction for a check like this to fail in.
REFERENCED_KEY = re.compile(r"CHECKS\[\s*[\"']([a-z][a-z0-9_]*)[\"']\s*\]")
APPLIED = re.compile(
    r"apply_checks_by_metadata\("
    r"(?:(?!apply_checks_by_metadata).){0,400}?"
    r"CHECKS\[\s*[\"']([a-z][a-z0-9_]*)[\"']\s*\]",
    re.DOTALL,
)

# The other two thirds of the per-dataset wiring. Counted rather than name-matched: the valid
# view and the quarantine table are named for the table they feed, not for the dataset key
# (whoz_profile_shaped's quarantine is whoz_profiles_quarantine), so there is no name to
# derive — but there must be exactly one of each per checked dataset, and a count catches both
# a missing pair and a stray extra one.
#
# Anchored on `<engine>.` so that prose in a comment ("get_valid() drops the result columns")
# does not count as wiring. The prefix is \w+ rather than a literal `dq` for the same reason
# the old expectation matcher used \w+ for the decorator's module: renaming the engine
# variable should not quietly turn this into a no-op.
GET_VALID = re.compile(r"\b\w+\.get_valid\(")
GET_INVALID = re.compile(r"\b\w+\.get_invalid\(")
QUARANTINE_PROPERTY = re.compile(r"[\"']quality[\"']\s*:\s*[\"']quarantine[\"']")


def transformation_sources() -> str:
    # rglob, not glob: the transformation modules sit one level further down, in bronze/ and
    # silver/, so a non-recursive glob would match nothing at all.
    modules = sorted(TRANSFORMATIONS.rglob("*.py"))
    assert modules, f"no transformation modules found under {TRANSFORMATIONS}"
    return "\n".join(path.read_text(encoding="utf-8") for path in modules)


def test_every_rule_set_is_applied_by_a_transformation():
    # Both directions of one invariant: the dataset keys in checks/*.yml and the datasets the
    # transformations actually check describe the same set.
    #
    #   key referenced but absent from CHECKS -> a typo, which would otherwise raise a
    #     KeyError at pipeline update rather than here, seconds after it was written
    #   dataset in CHECKS that no apply call applies -> checks that are declared,
    #     hygiene-checked, and enforced by nothing: dead data that reads as live
    sources = transformation_sources()
    referenced = set(REFERENCED_KEY.findall(sources))
    applied = set(APPLIED.findall(sources))

    unknown = sorted(referenced - set(CHECKS))
    assert not unknown, (
        f"transformations/ reads CHECKS[...] with keys that do not exist in "
        f"whoz_ingestion/checks/: {unknown}. The pipeline would fail with a KeyError at "
        f"update time."
    )

    unapplied = sorted(set(CHECKS) - applied)
    assert not unapplied, (
        f"these datasets have checks in whoz_ingestion/checks/ but no transformation passes "
        f"them to dq.apply_checks_by_metadata, so the pipeline enforces nothing: {unapplied}. "
        f"Add a <name>_checked view in transformations/, or delete the checks."
    )


def test_every_checked_dataset_has_a_valid_view_and_a_quarantine_table():
    # The rest of the three-object shape. Applying the checks produces a DataFrame carrying
    # _errors/_warnings and nothing else; without a get_valid the annotated rows would land in
    # the silver table with two extra columns, and without a get_invalid the quarantined rows
    # would be discarded silently — which is the whole thing DQX was adopted for.
    sources = transformation_sources()
    expected = len(CHECKS)

    assert len(GET_VALID.findall(sources)) == expected, (
        f"expected one dq.get_valid() per checked dataset ({expected}), found "
        f"{len(GET_VALID.findall(sources))}. Every _checked view needs exactly one consumer "
        f"that feeds the real table."
    )
    assert len(GET_INVALID.findall(sources)) == expected, (
        f"expected one dq.get_invalid() per checked dataset ({expected}), found "
        f"{len(GET_INVALID.findall(sources))}. A checked dataset with no quarantine table "
        f"drops its bad rows on the floor."
    )
    assert len(QUARANTINE_PROPERTY.findall(sources)) == expected, (
        f"expected one table_properties={{'quality': 'quarantine'}} per checked dataset "
        f"({expected}), found {len(QUARANTINE_PROPERTY.findall(sources))}. The property is how "
        f"a quarantine table is told apart from a silver one in the catalog."
    )

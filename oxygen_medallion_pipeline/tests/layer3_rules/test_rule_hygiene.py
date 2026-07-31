"""Layer 3 — are the data quality rules themselves right? (hygiene, over every rule)

The pipeline's quality rules live in utilities/expectations.py as {name: SQL} dicts and
are applied by the @dp.expect_all* decorators in transformations/. Those predicates are
strings: a rule naming a column that does not exist, or one that matches nothing it was
meant to catch, is invisible to py_compile, ruff and `databricks bundle validate` alike.
Worse than a broken test, a broken *rule* fails silently — the pipeline reports 100% pass
and nobody looks again.

So there are two kinds of test in this layer, and both matter:

  1. Hygiene, over every rule in the project via ALL_RULE_SETS. Automatic: add a rule to
     utilities/expectations.py and these start covering it with no edit here. What makes
     that true for a whole new rule set, and not just a new line in an existing one, is
     test_every_rule_set_is_registered, which checks the registry against the module —
     paired with test_every_rule_set_is_applied_by_a_transformation, which checks the other
     end, that a rule set the tests can see is one the pipeline actually runs.
  2. Behaviour, per rule set: it passes clean data (fixtures/whoz_profiles/typical.json,
     hazards.json) and it catches the rows it is meant to catch
     (fixtures/whoz_profiles/violations.json). This is the half that has to be written by
     hand, because only you know what a rule is supposed to mean.

This module is (1). The behaviour half, (2), is per entity: test_profile_rules.py and
test_talent_rules.py.

Coverage note: the profile rules are checked against real shape_profile() output
(test_profile_rules.py), so a typo in a column name fails there. The child-table rules
(aptitudes, positions, refs) get hygiene checks only — their SQL still lives inline in
transformations/silver/whoz_profile_children.py, so there is no local DataFrame with those
columns to resolve against. Moving those bodies into utilities/ the way
utilities/shaping/profile.py was done is what would upgrade them, and the tests to add
afterwards are the three in test_profile_rules.py.
"""

import pathlib
import re

import pytest
from pyspark.sql import functions as F
from utilities import expectations
from utilities.expectations import ALL_RULE_SETS

# Every (rule set label, rule name, predicate) in the project, flattened for
# parametrization so each rule shows up as its own named test case.
ALL_RULES = [
    pytest.param(label, name, sql, id=f"{label}.{name}")
    for label, rules in ALL_RULE_SETS.items()
    for name, sql in rules.items()
]

# Every module-level rule set in utilities/expectations.py, {name: rules}. Read off the
# module rather than written out here on purpose: a hand-kept list in this file would need
# exactly the maintenance — and fail in exactly the silent way — that the two tests below
# exist to prevent.
DECLARED_RULE_SETS = {
    name: rules
    for name, rules in vars(expectations).items()
    if name.endswith(("_MUST_HOLD", "_SHOULD_HOLD")) and isinstance(rules, dict)
}

TRANSFORMATIONS = pathlib.Path(__file__).parent.parent.parent / "src" / "whoz_ingestion_etl" / "transformations"


# -------------------------------------------------------------------------------------
# Hygiene — applies to every rule in the project, automatically.
# -------------------------------------------------------------------------------------
@pytest.mark.parametrize(("label", "name", "sql"), ALL_RULES)
def test_rule_is_well_formed(spark, label, name, sql):
    # snake_case names, because the name becomes a metric label in the pipeline's data
    # quality dashboard — the one place these are read by someone who wasn't looking at
    # the code.
    assert re.fullmatch(r"[a-z][a-z0-9_]*", name), f"{label}: rule names should be snake_case, got {name!r}"
    assert sql.strip(), f"{label}.{name}: empty predicate"

    # Parses as a Spark SQL expression. Catches unbalanced parentheses and typos like
    # "BETWEN" — not unresolved column names, which need a DataFrame (see below).
    F.expr(sql)


@pytest.mark.parametrize(("label", "name", "sql"), ALL_RULES)
def test_drop_rules_only_ever_guard_a_key(label, name, sql):
    # Policy check, deliberately strict. A *_MUST_HOLD rule maps to expect_all_or_drop,
    # which silently deletes rows: the pipeline reports a healthy run and the data is
    # simply not there. That is only ever acceptable for a row that cannot be used at all,
    # which in practice means a null primary key. Anything else belongs in *_SHOULD_HOLD,
    # where it is counted and visible.
    #
    # If you have a genuine reason to drop on something else, change this test in the same
    # commit — the point is that it takes a deliberate act, not that it is impossible.
    if not label.endswith(".must_hold"):
        return

    assert "IS NOT NULL" in sql.upper(), (
        f"{label}.{name} drops rows on a predicate that isn't a null-key check: {sql!r}. "
        f"Move it to the matching *_SHOULD_HOLD dict unless dropping is really intended."
    )


def test_rule_names_are_unique_across_the_project():
    # Two rules sharing a name on different tables is legal but reads as one metric in the
    # quality dashboard, which is how you end up debugging the wrong table.
    seen: dict[str, str] = {}
    collisions = []
    for label, rules in ALL_RULE_SETS.items():
        for name in rules:
            if name in seen:
                collisions.append(f"{name!r} in both {seen[name]} and {label}")
            seen[name] = label

    assert not collisions, "duplicate rule names: " + "; ".join(collisions)


def test_every_rule_set_is_registered():
    # The guard on the two parametrized tests above: they only see what ALL_RULE_SETS holds,
    # and that registry is maintained by hand. Add a rule to an existing dict and it is
    # covered automatically; add a whole new *_SHOULD_HOLD dict, wire it into a decorator in
    # transformations/, and forget the registry line, and nothing here covers it — with no
    # failure to say so. That is the same silent gap this module exists to close, one level
    # up, so close it the same way: ask the module what it declares.
    #
    # By identity, not equality: the registry holds the very same dict objects, and two rule
    # sets that happened to be equal (both empty, say) would otherwise cover for each other.
    registered = {id(rules) for rules in ALL_RULE_SETS.values()}
    missing = sorted(name for name, rules in DECLARED_RULE_SETS.items() if id(rules) not in registered)

    assert not missing, (
        f"these rule sets are declared in utilities/expectations.py but are not in "
        f"ALL_RULE_SETS, so no hygiene test above covers them: {missing}. Add one line per "
        f"dict to the registry."
    )


# A rule set being handed to one of the pipeline's expectation decorators, e.g.
#   @dp.expect_all_or_drop(TALENT_MUST_HOLD)
# Matched as text rather than by importing transformations/: those modules call dp.table()
# and friends at import time and rely on `spark` being injected by Lakeflow, so they cannot
# be imported outside a running pipeline. The module prefix is \w+ rather than a literal
# `dp` so that renaming the import alias does not quietly turn this test into a no-op.
#
# It is deliberately a decorator match and not a bare name search: a plain search would be
# satisfied by the `from utilities.expectations import ...` line at the top of each module,
# so a rule set that was imported and then never applied would pass. If this regex ever
# stops matching the real call style, every rule set reports as unapplied at once — loud and
# obviously wrong, which is the right direction for a check like this to fail in.
APPLIED_RULE_SET = re.compile(r"@\w+\.expect_all(?:_or_drop)?\(\s*([A-Z][A-Z0-9_]*)\s*[,)]")


def test_every_rule_set_is_applied_by_a_transformation():
    # The other half of the invariant. test_every_rule_set_is_registered proves a rule set
    # reaches the tests; this proves it reaches the pipeline. Registration alone buys only
    # the hygiene checks above — that the predicate parses, that the name is unique and
    # snake_case — and says nothing about whether the rule ever runs. So a dict that is
    # declared and registered but passed to no decorator is green everywhere while doing
    # precisely nothing: dead data that reads as live, which is worse than an unregistered
    # rule set, because that at least fails now.
    #
    # rglob, not glob: the transformation modules sit one level further down, in bronze/
    # and silver/, so a non-recursive glob would match nothing at all.
    modules = sorted(TRANSFORMATIONS.rglob("*.py"))
    assert modules, f"no transformation modules found under {TRANSFORMATIONS}"

    sources = "\n".join(path.read_text(encoding="utf-8") for path in modules)
    applied = set(APPLIED_RULE_SET.findall(sources))
    unapplied = sorted(set(DECLARED_RULE_SETS) - applied)

    assert not unapplied, (
        f"these rule sets are declared in utilities/expectations.py but no transformation "
        f"applies them, so the pipeline does not enforce them: {unapplied}. Pass each to "
        f"@dp.expect_all (or @dp.expect_all_or_drop) in transformations/, or delete it."
    )

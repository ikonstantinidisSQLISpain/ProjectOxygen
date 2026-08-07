"""Assertion helpers shared by the test modules.

Things this suite asserts over and over, each with a fiddly detail worth getting right
exactly once:

  * a DataFrame's schema matches a declared DDL string  -> assert_schema_matches_ddl
  * no DQX check skipped itself                         -> assert_no_skipped_checks
  * no row violates a set of DQX checks                 -> assert_no_dqx_violations
  * specific rows do violate them, as designed          -> assert_dqx_violations

Import them by plain module name (`from helpers import ...`). tests/ is on sys.path because
pyproject.toml's pythonpath names it — the test modules live in layer subfolders, so pytest's
own "directory of the test file" insertion would only reach the subfolder. There is
deliberately no __init__.py here, since adding one would make the import `tests.helpers`
instead and every test module would need editing.

The DQX helpers all take the *result* of `dq.apply_checks_by_metadata(df, checks)` — the
input DataFrame with `_errors` and `_warnings` appended — rather than a DataFrame and a rule
set, because applying the checks once and asserting several things about the one result is
both faster and closer to what the pipeline does.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, TimestampType

# DQX's default result column names. Not configured away anywhere in this project, and the
# transformations rely on the defaults too — if that ever changes, ExtraParams'
# result_column_names is the setting, and these two constants are the other half of it.
ERRORS = "_errors"
WARNINGS = "_warnings"

# The row identity the DQX counters group by. A result struct array carries no row key of
# its own, so counting *rows* rather than *result entries* needs one. It matters: a
# for_each_column check expands into one entry per column, so a row with both halves of a
# composite key null yields two entries under one name and would otherwise count twice —
# where the composite SQL predicate this replaced counted the row once.
_ROW_IX = "_dqx_row_ix"


def to_utc_strings(df: DataFrame, fmt: str = "yyyy-MM-dd HH:mm:ss.SSS") -> DataFrame:
    """Render every TIMESTAMP column as a string, so collected values are machine-independent.

    Use this on any DataFrame whose timestamps a test asserts on. The reason is a genuine
    trap, measured rather than assumed:

        spark.sql.session.timeZone = UTC   (pinned in conftest.py)
        JVM default timezone       = Europe/Madrid   (the developer's laptop)

        collected Python datetime  -> 2024-01-02 04:04:05   <- JVM default, machine-dependent
        Spark-side date_format     -> 2024-01-02 03:04:05   <- session tz, stable

    df.collect() converts TIMESTAMPs to Python datetimes using the *JVM default* zone, which
    the session setting does not govern. So a test asserting a collected datetime passes on
    a laptop in Madrid and fails on GitHub's UTC runners. Formatting in Spark first moves
    the conversion under the session zone, where it is pinned.

    Assert on the string. It reads better in a failure message anyway.
    """
    return df.select(
        *[
            F.date_format(field.name, fmt).alias(field.name)
            if isinstance(field.dataType, TimestampType)
            else F.col(field.name)
            for field in df.schema.fields
        ]
    )


def ddl_columns(ddl: str) -> list[tuple[str, str]]:
    """Parse a DDL string into an ordered [(name, type)] list.

    Raises on invalid DDL, which is itself a useful test — see
    tests/layer2_contract/test_profile_contract.py for why nothing else in the toolchain
    notices.
    """
    return [(f.name, f.dataType.simpleString()) for f in StructType.fromDDL(ddl).fields]


def df_columns(df: DataFrame) -> list[tuple[str, str]]:
    """The same ordered [(name, type)] shape as ddl_columns, for a real DataFrame."""
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]


def assert_schema_matches_ddl(df: DataFrame, ddl: str) -> None:
    """Assert df's columns match the DDL exactly: same names, same types, same order.

    Order is part of the contract, not pedantry: AUTO CDC matches a source view to its
    target table positionally as well as by name, so two columns of the same type
    swapping places is a real defect that a set comparison would wave through.
    """
    declared = ddl_columns(ddl)
    actual = df_columns(df)
    if declared == actual:
        return

    # A bare `assert declared == actual` on a 35-column schema prints two walls of
    # tuples and leaves you to spot the difference. Point at it instead.
    declared_names = [n for n, _ in declared]
    actual_names = [n for n, _ in actual]
    problems = []
    for name in declared_names:
        if name not in actual_names:
            problems.append(f"  - {name}: declared in the DDL, missing from the DataFrame")
    for name in actual_names:
        if name not in declared_names:
            problems.append(f"  - {name}: produced by the DataFrame, missing from the DDL")
    declared_types = dict(declared)
    for name, actual_type in actual:
        expected_type = declared_types.get(name)
        if expected_type is not None and expected_type != actual_type:
            problems.append(f"  - {name}: DDL says {expected_type}, DataFrame produces {actual_type}")
    if not problems:
        problems.append(
            f"  - same columns and types, different order:\n"
            f"      DDL:       {declared_names}\n"
            f"      DataFrame: {actual_names}"
        )

    raise AssertionError("schema does not match the declared DDL:\n" + "\n".join(problems))


# -------------------------------------------------------------------------------------
# DQX. Everything below operates on the result of apply_checks_by_metadata.
# -------------------------------------------------------------------------------------
def _all_results(result_df: DataFrame) -> DataFrame:
    """One row per (input row, fired check): _ROW_IX, name, message, skipped.

    _errors and _warnings are concatenated rather than examined separately, because every
    caller here wants "did this named check fire on this row", and criticality is already
    recorded in the checks YAML. A passing row gets NULL, not an empty array, so both need
    coalescing before concat — that is a DQX behaviour, not a defensive habit.
    """
    for column in (ERRORS, WARNINGS):
        if column not in result_df.columns:
            raise AssertionError(
                f"{column!r} is not a column of this DataFrame, so it is not the result of "
                f"apply_checks_by_metadata. Columns: {result_df.columns}"
            )

    empty_errors = F.array().cast(result_df.schema[ERRORS].dataType)
    empty_warnings = F.array().cast(result_df.schema[WARNINGS].dataType)
    return (
        result_df.withColumn(_ROW_IX, F.monotonically_increasing_id())
        .select(
            _ROW_IX,
            F.explode(
                F.concat(F.coalesce(F.col(ERRORS), empty_errors), F.coalesce(F.col(WARNINGS), empty_warnings))
            ).alias("_result"),
        )
        .select(
            _ROW_IX,
            F.col("_result.name").alias("name"),
            F.col("_result.message").alias("message"),
            F.col("_result.skipped").alias("skipped"),
        )
    )


def assert_no_skipped_checks(result_df: DataFrame) -> None:
    """Assert DQX evaluated every check rather than quietly skipping any of them.

    THE MOST IMPORTANT ASSERTION IN THIS FILE, and the one that has no equivalent in the
    pipeline. DQX resolves a check's columns at apply time, and when it cannot — a typo'd
    `column:`, a `filter:` or an `sql_expression` naming something that is not there — it
    does not fail. It SKIPS the check and emits a result struct with `skipped=true` on
    every row. Both directions of that are silent in production:

        criticality: error -> 100% of rows are quarantined and the silver table empties,
                              while the pipeline reports a successful update
        suppress_skipped   -> the check becomes a no-op and reports a clean 100% pass
                              forever, which is the exact failure quality rules exist to
                              prevent

    DQEngine.validate_checks does not catch it either: it validates function names,
    argument names and criticality, none of which need a DataFrame. Only applying the
    checks to real columns does, which is why this is a test-suite assertion and why every
    behaviour test in tests/layer3_rules/ calls it — including the ones whose point is that
    checks *do* fire, since a skipped check is not a fired one.

    TWO LIMITS, both measured rather than reasoned about, because this assertion is easy to
    over-trust and both of them make it pass on a check it should have caught:

      1. ZERO ROWS. DQX decides to skip at analysis time but *reports* it per row —
         `_build_result_struct(..., skipped=True)` is a Column expression, so a result with
         no rows carries no skip markers and there is nothing here to find. Mutating
         `position_id` to `positon_id` in checks/whoz_position_aptitude_refs.yml fails this
         on the `typical` fixture (2 rows) and passed silently on `hazards` (0 rows) until
         the guard below existed. An empty result makes every assertion in this file vacuous,
         so it is now a failure rather than a pass.
      2. suppress_skipped. Under `ExtraParams(suppress_skipped=True)` DQX emits no marker at
         all and this assertion cannot see the skip by any means. It is not a guard against
         that flag — it works *because* conftest.py leaves the flag off. Do not turn it on
         here without replacing this assertion with something else.
    """
    # A vacuous pass reads exactly like a real one in pytest output, and this is the assertion
    # the whole layer leans on — so an empty result is treated as a broken test, not a clean
    # one. Callers with a legitimately empty case should say so explicitly at the call site.
    if result_df.isEmpty():
        raise AssertionError(
            "this result has no rows, so it proves nothing: DQX reports `skipped` per row, and "
            "with no rows there is no marker to find. A typo'd column name would pass here. "
            "Feed the checks a fixture that actually produces rows, or assert the emptiness "
            "deliberately at the call site instead of routing it through this helper."
        )

    skipped = _all_results(result_df).filter(F.col("skipped")).select("name", "message").distinct().collect()
    if not skipped:
        return

    columns = [c for c in result_df.columns if c not in (ERRORS, WARNINGS)]
    detail = "\n".join(f"  - {row['name']}: {row['message']}" for row in sorted(skipped, key=lambda r: r["name"]))
    raise AssertionError(
        "DQX skipped these checks instead of evaluating them, which in the pipeline would "
        "either quarantine every row or check nothing at all — both silently:\n"
        f"{detail}\n"
        f"columns actually available: {columns}"
    )


def dqx_failing_counts(result_df: DataFrame, checks: list[dict]) -> dict[str, int]:
    """For each named check, how many rows it fired on. Zero for checks that never fired.

    Every name in `checks` is present in the result, which is what lets the assertions below
    keep the complete-dict semantics the SQL-predicate versions had: a check that fires on
    nothing has to be visible as a 0, not as an absent key.

    Rows, not result entries — see the note on _ROW_IX.
    """
    declared = [check["name"] for check in checks]
    counted = {
        row["name"]: row["n"]
        for row in _all_results(result_df).groupBy("name").agg(F.countDistinct(_ROW_IX).alias("n")).collect()
    }

    # A name in the result that no check declares means the checks list and the result came
    # from different applies — an easy mistake to make once a module holds several datasets,
    # and one that would otherwise show up as a mysteriously missing violation.
    unexpected = sorted(set(counted) - set(declared))
    if unexpected:
        raise AssertionError(
            f"this result carries check names that are not in the checks list passed here: "
            f"{unexpected}. Declared: {sorted(declared)}. Did the DataFrame and the checks "
            f"come from the same apply_checks_by_metadata call?"
        )

    return {name: counted.get(name, 0) for name in declared}


def assert_no_dqx_violations(result_df: DataFrame, checks: list[dict]) -> None:
    """Assert every row satisfies every check — and that every check was actually evaluated."""
    assert_no_skipped_checks(result_df)

    failures = {name: n for name, n in dqx_failing_counts(result_df, checks).items() if n}
    assert not failures, (
        f"expected no check violations, got {failures} "
        f"(check name -> failing row count, out of {result_df.count()} rows)"
    )


def assert_dqx_violations(result_df: DataFrame, checks: list[dict], expected: dict[str, int]) -> None:
    """Assert exactly the expected checks fire, exactly the expected number of times.

    `expected` is the complete picture, not a subset: any check not listed must have zero
    failures. That is what makes this catch an over-broad check — one that also flags rows it
    was never meant to — and not just an under-broad one.
    """
    assert_no_skipped_checks(result_df)

    actual = dqx_failing_counts(result_df, checks)
    complete = {name: expected.get(name, 0) for name in actual}
    assert actual == complete, (
        f"check violations did not match:\n"
        f"  expected: {complete}\n"
        f"  actual:   {actual}"
    )

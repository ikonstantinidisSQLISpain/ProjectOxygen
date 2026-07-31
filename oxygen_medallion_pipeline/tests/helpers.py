"""Assertion helpers shared by the test modules.

Three things this suite asserts over and over, each with a fiddly detail worth getting
right exactly once:

  * a DataFrame's schema matches a declared DDL string  -> assert_schema_matches_ddl
  * no row violates a set of quality rules              -> assert_no_violations
  * specific rows do violate them, as designed          -> assert_violations

Import them by plain module name (`from helpers import ...`). tests/ is on sys.path because
pyproject.toml's pythonpath names it — the test modules live in layer subfolders, so pytest's
own "directory of the test file" insertion would only reach the subfolder. There is
deliberately no __init__.py here, since adding one would make the import `tests.helpers`
instead and every test module would need editing.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, TimestampType


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


def failing_counts(df: DataFrame, rules: dict[str, str]) -> dict[str, int]:
    """For each {name: SQL predicate} rule, how many rows fail it.

    A row fails when the predicate is not TRUE — FALSE *and* NULL both count. That is
    the strict reading, and it is why utilities/expectations.py requires every warn-level
    rule to be written null-safe ("x IS NULL OR <check>"): under this counting, a rule
    that isn't null-safe reports every row with a missing optional field as a violation.
    Writing them null-safe means a violation always means what it says.

    Evaluated one rule at a time rather than as a single aggregate. Fixtures are a
    handful of rows, so the extra Spark jobs cost nothing, and an unresolved column name
    then fails with the rule that caused it named in the message instead of a bare
    AnalysisException pointing somewhere inside a 10-column aggregate.
    """
    counts = {}
    for name, sql in rules.items():
        try:
            counts[name] = df.filter(~F.expr(sql).eqNullSafe(F.lit(True))).count()
        except Exception as exc:
            # First line only. Spark appends the whole unresolved logical plan to an
            # AnalysisException's message — several hundred lines for a 35-column
            # DataFrame, and never the useful part. The first line is where
            # UNRESOLVED_COLUMN.WITH_SUGGESTION puts its "Did you mean ...?" hint. The
            # full exception is still chained below if it is ever genuinely needed.
            cause = str(exc).split("\n")[0]
            raise AssertionError(
                f"rule {name!r} could not be evaluated against this DataFrame.\n"
                f"  predicate: {sql}\n"
                f"  columns available: {df.columns}\n"
                f"  cause: {cause}"
            ) from exc
    return counts


def assert_no_violations(df: DataFrame, rules: dict[str, str]) -> None:
    """Assert every row in df satisfies every rule."""
    failures = {name: n for name, n in failing_counts(df, rules).items() if n}
    assert not failures, (
        f"expected no rule violations, got {failures} "
        f"(rule name -> failing row count, out of {df.count()} rows)"
    )


def assert_violations(df: DataFrame, rules: dict[str, str], expected: dict[str, int]) -> None:
    """Assert exactly the expected rules fire, exactly the expected number of times.

    `expected` is the complete picture, not a subset: any rule not listed must have zero
    failures. That is what makes this catch an over-broad rule — one that also flags rows
    it was never meant to — and not just an under-broad one.
    """
    actual = failing_counts(df, rules)
    complete = {name: expected.get(name, 0) for name in actual}
    assert actual == complete, (
        f"rule violations did not match:\n"
        f"  expected: {complete}\n"
        f"  actual:   {actual}"
    )

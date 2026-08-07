"""Layer 1 — cross-cutting shaping hygiene, over every module in whoz_ingestion/shaping/.

The per-entity files in this folder answer "does shape_<entity>() do what we intended". This
one answers a question none of them can, because the answer differs between the engine the
tests run on and the engine production runs on.

WHY THIS FILE EXISTS: `size(NULL)` IS NOT PORTABLE.

    Databricks Runtime / serverless   size(NULL) = -1
    local open-source Spark 4.0       size(NULL) = NULL

Every `*_count` column in this project was written assuming the second. On a deployed
pipeline they landed as -1: `silver.whoz_talents.tag_count` on 3,629 of 4,116 rows,
`aspiration_count` on 2,656, and on the user entity `coalesce(size(<map>), size(<array>))`
never reached its second branch at all — -1 is a value coalesce is happy to stop at — which
fired `federation_count_at_most_one` on 1,173 real accounts.

A BEHAVIOURAL TEST CANNOT CATCH THIS, and that was measured rather than assumed.
`spark.sql.legacy.sizeOfNull` is a no-op in Spark 4.0 — setting it true still yields NULL —
so there is no way to make the local session behave like Databricks. A test written that way
passes identically against the broken and the fixed implementation, which makes it worse than
no test.

So the guard here is STRUCTURAL: `size()` is never called on a possibly-NULL collection
anywhere in shaping/, because every call goes through `collection_size_sql`, which spells the
NULL case out with a CASE expression that means the same thing on both engines.
"""

import ast
import io
import pathlib
import re
import tokenize

from whoz_ingestion.shaping import collection_size_sql

SHAPING = pathlib.Path(__file__).parent.parent.parent / "src" / "whoz_ingestion" / "shaping"

# `size(` not immediately preceded by the `ELSE ` of collection_size_sql's CASE expression.
# That is the whole rule: a guarded size() is fine, a bare one is the bug.
UNGUARDED_SIZE = re.compile(r"(?<!ELSE )\bsize\(")


def code_without_comments_or_docstrings(path: pathlib.Path) -> str:
    """Module source with comments and docstrings removed, other string literals kept.

    Both exclusions are needed and neither is optional. Comments and docstrings in these
    modules legitimately *discuss* `size(cast(...))` — this file's own header does — and a
    naive grep would flag the explanation as the offence. But the SQL bodies in the child-query
    modules are ordinary f-strings that must still be scanned, so strings cannot be dropped
    wholesale.
    """
    source = path.read_text(encoding="utf-8")
    docstring_lines = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            body = getattr(node, "body", None)
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                docstring_lines.update(range(body[0].lineno, (body[0].end_lineno or body[0].lineno) + 1))

    kept = []
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.COMMENT:
            continue
        if token.type == tokenize.STRING and token.start[0] in docstring_lines:
            continue
        kept.append(token.string)
    return "\n".join(kept)


def sql_comments_stripped(text: str) -> str:
    """Drop `-- ...` SQL comments, which the child queries use to explain themselves."""
    return "\n".join(re.sub(r"--.*$", "", line) for line in text.splitlines())


def test_collection_size_sql_spells_out_the_null_case():
    # The helper's contract, pinned as a pure string function — no Spark, no engine, so this
    # assertion means the same thing everywhere.
    sql = collection_size_sql("my_collection")

    assert "IS NULL" in sql and "THEN NULL" in sql, (
        f"collection_size_sql must handle NULL explicitly rather than relying on size(NULL), "
        f"which is -1 on Databricks and NULL locally. Got: {sql}"
    )
    assert not UNGUARDED_SIZE.search(sql), f"the size() call must sit behind the ELSE branch. Got: {sql}"


def test_no_shaping_module_calls_size_on_a_possibly_null_collection():
    # The reason this is a source scan and not a behaviour assertion is in the module
    # docstring: the local engine physically cannot reproduce the production behaviour.
    offenders = []
    for path in sorted(SHAPING.glob("*.py")):
        if path.name == "__init__.py":
            continue  # where collection_size_sql is defined; its own size() is the guarded one
        code = sql_comments_stripped(code_without_comments_or_docstrings(path))
        for lineno, line in enumerate(code.splitlines(), start=1):
            if UNGUARDED_SIZE.search(line):
                offenders.append(f"  {path.name} (approx. line {lineno}): {line.strip()[:80]}")

    assert not offenders, (
        "these call size() on a collection that may be NULL:\n"
        + "\n".join(offenders)
        + "\n\nsize(NULL) is -1 on Databricks and NULL on the local engine, so this passes "
        "every local test and lands -1 in silver where the schema promises NULL. Use "
        "whoz_ingestion.shaping.collection_size_sql instead."
    )

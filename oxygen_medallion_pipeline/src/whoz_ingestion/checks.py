"""Data quality checks as data: checks/<dataset>.yml in, {dataset: [DQX check, ...]} out.

Every file under checks/ is a **native Databricks Labs DQX check list** — the exact shape
`DQEngine.validate_checks` and `apply_checks_by_metadata` accept, and that DQX's own
`FileChecksStorageConfig` would load unmodified. Nothing here translates a private format
into DQX's; the files are already in it, and this module only finds them, keys them by
dataset, and refuses to hand back a set it cannot vouch for.

Two things read CHECKS, and that dual readership is the whole point:

  1. the pipeline, via `dq.apply_checks_by_metadata(df, CHECKS[dataset])` in a
     `<dataset>_checked` view in transformations/silver/*.py
  2. the test suite, tests/layer3_rules/, which applies the same lists to real
     shape_profile()/shape_talent()/child-query output — see README.md#data-quality-checks

THE CRITICALITY CONTRACT, and what it costs.

    error -> the row is excluded from `get_valid` and lands in the dataset's quarantine
             table instead. It is NOT in the silver table any more. Only ever for a row
             that cannot be used at all, which in practice means a null primary key.
             tests/layer3_rules/test_rule_hygiene.py enforces that policy.
    warn  -> the row is in BOTH. It flows into the silver table as normal and is *also*
             written to quarantine, carrying the `_warnings` struct that says why.
             Everything else: assumptions about the source that hold today and should
             raise an eyebrow, not remove data, if they ever stop.

So the quarantine table is not a dead-letter queue. It is "every row anything fired on",
and a row in it may well also be in the silver table. Query `_errors IS NOT NULL` to see
only the rows that were actually withheld.

WHY VALIDATION IS HERE RATHER THAN IN A TEST. `DQEngine.validate_checks` is a genuine
staticmethod — no Spark session, no workspace client, no network — so running it at import
costs nothing and means a malformed check fails a pipeline update and a pytest run
identically, on the same input, with the same message. It catches an unknown function name,
an unexpected argument name, and an invalid criticality.

WHAT IT DOES NOT CATCH, and this is the important part: a column that does not exist.
DQX resolves columns at apply time and degrades *softly* — an unresolvable column/filter/
expression makes DQX skip the check and emit `skipped=true` on every row instead of
failing. At `error` criticality that quarantines 100% of rows; under
`ExtraParams(suppress_skipped=True)` it becomes a silent no-op. Neither is loud. Nothing
in this module can see it, because seeing it needs a DataFrame. `tests/helpers.py`'s
`assert_no_skipped_checks` is what closes that hole, and every layer-3 behaviour test calls
it. If you add a check here, add it to a behaviour test too.
"""

from pathlib import Path

import yaml
from databricks.labs.dqx.engine import DQEngine

CHECKS_DIR = Path(__file__).parent / "checks"


def _load_file(path: Path) -> list[dict]:
    """Read one checks/<dataset>.yml into the list of check dicts DQX takes."""
    # From an open file, not read_text(): PyYAML names the stream in a syntax error, so the
    # message points at this file rather than at `"<unicode string>", line 138`.
    with path.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)

    if not isinstance(document, list) or not document:
        raise ValueError(
            f"{path} must hold a non-empty DQX check list — a top-level YAML sequence of "
            f"`- name: / criticality: / check:` entries. Got {type(document).__name__}."
        )

    for position, check in enumerate(document):
        where = f"{path}, check {position}"
        if not isinstance(check, dict):
            raise TypeError(f"{where}: each check must be a mapping, got {type(check).__name__}")

        # An explicit name on every check, because DQX will happily generate one from the
        # function and column and we would not notice. The names here are referenced by the
        # expected-count dicts in tests/layer3_rules/ and surface as metric labels, so a
        # generated name is a silently renamed metric and a silently broken test.
        name = check.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                f"{where}: every check needs an explicit non-empty `name:`. DQX generates one "
                f"otherwise, and the generated name is not the one the tests and the metric "
                f"labels use."
            )

    return document


def _load(directory: Path) -> dict[str, list[dict]]:
    """Every checks/*.yml, keyed by filename stem, validated as a whole."""
    paths = sorted(directory.glob("*.yml"))
    if not paths:
        raise FileNotFoundError(f"no check files found in {directory}. Expected one <dataset>.yml per dataset.")

    loaded = {path.stem: _load_file(path) for path in paths}

    # DQX's own static validation, per dataset, naming the file. Unknown function, unexpected
    # argument name, invalid criticality. It returns a status rather than raising, so the
    # raise is ours — and it is a raise rather than a warning on purpose: a check set that
    # cannot be applied should stop a pipeline update, not be skipped by it.
    problems = []
    for dataset, checks in loaded.items():
        status = DQEngine.validate_checks(checks)
        if status.has_errors:
            problems.extend(f"  {dataset} ({directory / f'{dataset}.yml'}): {error}" for error in status.errors)
    if problems:
        raise ValueError("DQX rejected these checks:\n" + "\n".join(problems))

    # Names unique across every dataset, not just within one. Two checks sharing a name on
    # different tables is legal and reads as one metric, which is how you end up debugging
    # the wrong table. Checked here as well as in test_rule_hygiene.py so that a collision
    # introduced in the pipeline's copy of these files fails at update time too.
    seen: dict[str, str] = {}
    collisions = []
    for dataset, checks in loaded.items():
        for check in checks:
            name = check["name"]
            if name in seen:
                collisions.append(f"  {name!r}: in both {seen[name]} and {dataset}")
            seen[name] = dataset
    if collisions:
        raise ValueError("check names must be unique across every dataset:\n" + "\n".join(collisions))

    return loaded


CHECKS: dict[str, list[dict]] = _load(CHECKS_DIR)

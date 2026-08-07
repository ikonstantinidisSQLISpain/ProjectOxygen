"""One DQEngine for the whole pipeline, constructed lazily on first use.

WHY THIS EXISTS AT ALL — and it is not tidiness, it is a measured cost.

`DQEngine.__init__` verifies workspace connectivity as a side effect of construction.
`DQEngineBase._verify_workspace_client` calls `ws.clusters.select_spark_version()`
unconditionally, with no try/except, purely to prove the workspace is reachable — and
`DQEngine.__init__` triggers it **twice**, once for itself and once for the `DQEngineCore` it
builds. So every `DQEngine(...)` in this project is 2 blocking control-plane round trips
during pipeline graph initialisation, any of which raises and fails the update.

That was 6 round trips when the silver layer was 3 modules. Splitting it to one module per
table would have made it ~16 for no benefit, since every one of them builds an identical
engine. One shared engine keeps it at 2 no matter how many dataset modules there are.

WHY LAZY, and this is the part that must not be "simplified" away. This module lives in
`whoz_ingestion`, which pytest imports — `tests/layer3_rules/` reaches it through
`whoz_ingestion.checks`. Constructing a `WorkspaceClient()` at import time would therefore
need Databricks credentials to *collect* the test suite, which is precisely the property the
`whoz_ingestion` / `whoz_ingestion_etl` split exists to protect. Behind a function, the
construction only happens when a transformation module actually asks for it, inside a running
pipeline. Nothing in the test suite calls `engine()`; `tests/conftest.py` builds its own
`DQEngine` over a `MagicMock(spec=WorkspaceClient)` instead.

Consequence worth knowing: **this path is exercised for the first time at deploy.** No local
test can reach it, and `databricks bundle validate` does not authenticate.

WHY `spark` IS A PARAMETER rather than read here. Lakeflow injects `spark` into each
transformation module's namespace; it is not importable from this side of the seam. Passing
the injected session in is the difference between "correct" and "correct by coincidence" —
`DQEngine` would otherwise fall back to `SparkSession.builder.getOrCreate()`, which happens to
be the same session today and is not guaranteed to stay so.

NO ExtraParams, deliberately. `run_time_overwrite` / `run_id_overwrite` exist because those two
fields are non-deterministic per run, which breaks incremental refresh of a MATERIALIZED VIEW
built on DQX output. Nothing downstream of DQX in this pipeline is a materialized view: every
checked dataset is a temporary view or a streaming table, and a streaming table writes each row
exactly once, so a per-run timestamp is a fact about that row rather than a value that has to
stay stable across recomputation. The only materialized views here are the `*_payload_shapes`
drift monitors in `transformations/bronze/`, which carry no checks. If a materialized view is
ever built over a quarantine table, pin both values then.
"""

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

# Module-level, so the one instance is shared by every transformation module in the pipeline's
# Python process. Not a functools.lru_cache: the cache key would be the SparkSession, and two
# engines over the same session is exactly what this exists to prevent.
_engine: DQEngine | None = None


def engine(spark: SparkSession) -> DQEngine:
    """The pipeline's DQEngine, built on first call and reused thereafter.

    Call this from a transformation module with the `spark` Lakeflow injected:

        from whoz_ingestion.dq import engine
        dq = engine(spark)

    Do NOT call it from a test. It constructs a real `WorkspaceClient()` and will fail without
    Databricks credentials — use the `dq_engine` fixture in tests/conftest.py, which wires a
    DQEngine to the local session over a mocked client.
    """
    global _engine
    if _engine is None:
        _engine = DQEngine(WorkspaceClient(), spark=spark)
    return _engine

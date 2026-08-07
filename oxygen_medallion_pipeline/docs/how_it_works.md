# How it works — the pipeline and its tests

Orientation for someone who has to change this pipeline and wants to know *why it is shaped
the way it is* before they do. Read this once; it is the mental model the other two documents
assume.

**This document owns the reasoning. It deliberately does not own the detail:**

| For | Read |
|---|---|
| The mental model — why the folders, layers and checks are arranged this way | **this document** |
| The step-by-step procedure for adding an export file | [`adding_a_source_entity.md`](adding_a_source_entity.md) — 13 numbered steps |
| Table inventories, environments, CI/CD, how to query quarantine | [`../README.md`](../README.md) |
| The Whoz field inventory and every type hazard behind the casts | [`whoz_profile_data_model.md`](whoz_profile_data_model.md) |

If something here contradicts one of those, they win and this file is stale — say so in the PR
rather than fixing it in two places.

---

## 1. The constraint everything follows from

`pyspark.pipelines` (imported as `dp` in every file under `transformations/`) only fully exists
**inside a running Lakeflow pipeline**. Outside one, the module-level `spark.conf.get(...)`
calls raise and `DQEngine(WorkspaceClient())` needs real credentials. Anything that imports it
is permanently unreachable from pytest.

Every structural decision in this repo is downstream of that one fact:

```
src/whoz_ingestion_etl/          # imports pyspark.pipelines -> UNTESTABLE, keep it trivial
  transformations/
    bronze/<entity>.py           # Auto Loader ingestion + drift monitor
    silver/<table>.py            # ONE FILE PER TABLE — the three-object DQX shape,
                                 #   and for an entity's own grain, its AUTO CDC flows

src/whoz_ingestion/              # imports nothing pipeline-only -> TESTABLE, put logic here
  shaping/<entity>.py            # shape_<entity>(bronze_df) -> DataFrame
  shaping/<table>.py             # one module per exploded child table, as SQL *text*,
                                 #   named for the entity that owns it
  schemas/<entity>.yml           # each table's columns, in order, as data
  contract.py                    # renders those columns into the DDL string Lakeflow takes
  checks/<dataset>.yml           # one dataset's quality checks, as a native DQX check list
  checks.py                      # loads and validates them all -> CHECKS
  dq.py                          # the pipeline's one DQEngine, built lazily
```

**The naming rule on both sides is "filename == the table it produces".** So the profile
entity is nine files rather than three, and "where is `silver.whoz_profile_positions`
defined" needs no search:

```
transformations/silver/            shaping/
  whoz_profile.py                    profile.py
  whoz_profile_aptitudes.py          profile_aptitudes.py
  whoz_profile_positions.py          profile_positions.py
  whoz_position_aptitude_refs.py     profile_position_aptitude_refs.py
  whoz_profile_skill_ratings.py      profile_skill_ratings.py
  whoz_profile_completion_rules.py   profile_completion_rules.py
  whoz_talent.py                     talent.py
  whoz_talent_workspace_history.py   talent_workspace_history.py
  whoz_user.py                       user.py
  whoz_user_workspace_roles.py       user_workspace_roles.py
```

The entity prefix on the `shaping/` side is load-bearing. Those six child queries were one
`children.py` until recently, which filed five *profile* collections and one *talent*
collection (`workspace_history_sql`) under a single name implying they were siblings. They
explode out of different bronze tables. There is deliberately no `children/` folder: "child
table" is a relationship, not a category worth grouping by.

**One `DQEngine` for the whole pipeline, in `dq.py`, and it is lazy.** Constructing one is a
live workspace call — `_verify_workspace_client` runs `ws.clusters.select_spark_version()`
unconditionally, twice per construction — so one shared instance keeps the silver layer at 2
control-plane round trips instead of one pair per file. It is behind a function because
`whoz_ingestion` is imported by pytest, and constructing a `WorkspaceClient()` at import would
make collecting the suite require Databricks credentials.

They are **siblings under `src/`, not nested**, because the shared package has two consumers —
the pipeline and the test suite — and belongs to neither. The pipeline's `root_path` is `src/`
and `pyproject.toml`'s `pythonpath` names the same folder, so `whoz_ingestion.shaping.talent`
is one import path in a running pipeline and in pytest alike. **Keep those two settings in
step**: that they agree is what makes a wrong prefix fail on your laptop instead of at deploy.

> **The practical rule.** A file under `transformations/` should be a thin wrapper — read the
> upstream table, call a `whoz_ingestion` function, return the result. Every line of logic you
> put there is a line no test can reach.

`transformations/` is **layer-first** (`bronze/`, `silver/`) rather than entity-first, because a
future `gold/` joins *across* entities and needs a peer folder to live in. `tests/` mirrors
that: a folder per layer, the entity in the filename.

---

## 2. The unit of extension is the entity

Whoz exports several files. Each file is an **entity**: its own Auto Loader stream, its own
schema location, its own bronze and silver tables, its own fixtures, its own test file per
layer. Two exist today (`profile`, `talent`). A third gets a third set of files — never extra
rows in an existing table, and never a branch inside an existing function.

One entity's flow:

```
landing volume  (shared across entities, separated by filename glob)
   │   Auto Loader: multiLine + singleVariantColumn + its OWN schemaLocation
   ▼
bronze.whoz_<entity>s              one row per object, whole JSON in a VARIANT `payload`
   │                               + identity columns via try_variant_get
   │                               + <table>_payload_shapes  (drift monitor)
   ▼   shape_<entity>()            the one testable function
<entity>_checked                   @dp.temporary_view: DQX appends _errors / _warnings
   ├── get_valid  ──▶ <entity>_shaped ──▶ AUTO CDC ──▶ silver.whoz_<entity>s      (SCD1)
   │                                              └──▶ silver.whoz_<entity>_history (SCD2)
   └── get_invalid ─────────────────────────────────▶ silver.whoz_<entity>s_quarantine
```

Three things about this that are not obvious:

**Bronze infers nothing.** Every export is a pretty-printed JSON **array** (not JSONL) and is
polymorphic in several fields, so bronze reads it with `multiLine` + `singleVariantColumn` and
stores the payload uncast. Nothing is inferred, so nothing can drift. Silver casts lazily with
`try_variant_get`, which returns `NULL` rather than raising on a missing path — so one
malformed record still lands instead of killing the batch.

**Each entity needs its own `schemaLocation`.** Two Auto Loader streams must never share a
checkpoint directory. They may share the landing volume; they may not share that.

**A pipeline view is not materialized.** Databricks recomputes it per consumer, so
`<entity>_checked` is evaluated once per downstream flow, each with its own checkpoint. The
consequence worth knowing is not the cost — it is that **silver and quarantine advance on
separate checkpoints**, so "a warn row is in both" is *eventually* true, not atomically true.

---

## 3. Developing a new incoming file

[`adding_a_source_entity.md`](adding_a_source_entity.md) is the checklist — 13 numbered steps
(0–12), what each file must contain, and which test catches each mistake. Follow it there. What
follows is only the shape of it, so you know where you are.

Every step is tagged `[PIPELINE]` (ships to Databricks) or `[TESTS]` (never leaves your laptop
and CI). They **interleave deliberately**: each test step covers the pipeline step immediately
before it, which is what keeps the feedback loop short.

| Phase | Steps | What you are doing |
|---|---|---|
| Analyse | 0 | `docs/<source>_<entity>_data_model.md` — field inventory and type hazards, *before* code |
| Fixtures | 1–2 | Three JSON files, plus three edits to `tests/conftest.py` |
| **The real work** | **3–4** | `schemas/<entity>.yml` + `shaping/<entity>.py`, then `checks/<dataset>.yml` |
| The safety net | 5–7 | One new test file per layer |
| Wiring | 8–10 | Bronze + silver transformations, pipeline `configuration` entries |
| Deploy | 11 | The only verification steps 8–10 ever get |
| Docs | 12 | Both READMEs, the data model doc, and the header of every file you created |

Steps 3 and 4 are the work; steps 8 and 9 are wiring. That asymmetry is the point of the
folder split — and it is also why **steps 8 and 9 are where a mistake survives to production**,
since the `[TESTS]` track can only reach as far as the `whoz_ingestion/` seam.

### The three edits to `conftest.py`

```python
SOURCE_FILES  = {"whoz_talents": "dbfs:/Volumes/.../..._talent_report_anonymized.json"}
BRONZE_KEYS   = {"whoz_talents": {"talent_id": "$.id", "federation_id": "$.federationId"}}
# ...plus two one-line fixtures at the bottom of the file
```

> **`BRONZE_KEYS` must mirror the `try_variant_get` paths in your bronze transformation, and
> nothing checks that it does.** If they diverge, the tests build their input by a different
> route than production reads it — the suite stays green while production is wrong. Verify this
> one by eye, every time.

---

## 4. Fixtures: the convention is the contract

`fixtures/<entity>/` holds three files, and the filenames carry the meaning:

| File | Contains | Every check must |
|---|---|---|
| `typical.json` | ordinary, fully-populated records, realistic 24-hex ids | **pass** |
| `hazards.json` | one record per documented type hazard — polymorphic fields, absent vs. null vs. all-null objects, every timestamp precision, near-empty records | **pass** — *awkward is not invalid* |
| `violations.json` | records built to trip the checks | **fire**, with exactly the expected counts |

The `typical`/`hazards` split encodes a claim you would otherwise argue about in review: **a
weird-but-legitimate record must not trigger a quality check.** A false positive in a quality
check is worse than no check, because people stop reading the quarantine table.

Records in `hazards`/`violations` use descriptive ids (`hazard-absent-headline`,
`violation-profile-as-array`) rather than realistic ObjectIds, so assertions read as the thing
being tested.

Per entity, `conftest.py` gives you a file loader and an inline builder:

```python
def test_something(talent_fixture):     # a stored fixture file
    result = shape_talent(talent_fixture("hazards"))

def test_one_thing(talent_bronze):      # inline, for a pointed case
    result = shape_talent(talent_bronze([{"id": "t1", "profile": {"id": "p1"}}]))
```

Use a file when the records describe the *source* ("this is what Whoz sends"); inline when they
describe the *test* ("one field set, to isolate one behaviour").

### Two traps `conftest.py` exists to keep you out of

**Never build a `VARIANT` by hand.** `bronze_of` is the only place in the suite that constructs
one. Round-trip a VARIANT through a local `Row` (via `.first()`, or by collecting and
re-creating) and Spark loses the logical type, re-infers the physical
`STRUCT<metadata: BINARY, value: BINARY>` layout, and `try_variant_get` then fails with
`DATATYPE_MISMATCH` on JSON that is perfectly fine.

**Never assert on a collected timestamp.** Put the DataFrame through `helpers.to_utc_strings()`
first. `conftest.py` pins `spark.sql.session.timeZone` to UTC, but that governs Spark-side
rendering only — `df.collect()` converts `TIMESTAMP`s using the **JVM default** zone, which the
setting does not reach. Measured on this project: the same value reads `04:04:05` on a laptop
in Europe/Madrid and `03:04:05` on GitHub's UTC runners, so the assertion passes locally and
fails in CI.

---

## 5. The three test layers

Tests run against a **local, open-source PySpark session** — no workspace, no credentials, no
cluster. This works because Apache Spark 4.0 open-sourced `VARIANT`, `try_variant_get` and
`variant_explode` from Databricks Runtime, and that is all the tested logic uses. You need a
local JDK 17+ on `PATH`; nothing else.

Each layer answers one question, and the question is the reason the layer exists. They are
independent — a change usually only touches one.

| Layer | Folder | Question | What breaks if it is missing |
|---|---|---|---|
| **1. Shaping** | `tests/layer1_shaping/` | Does the transformation code do what we intended? | A parsing bug ships. The ordinary kind of test. |
| **2. Schema contract** | `tests/layer2_contract/` | Does the declared schema still describe what the code produces? | The schema Lakeflow takes is a raw DDL **string**. `py_compile`, `ruff`, `pytest` and `bundle validate` all pass green on one that cannot parse. |
| **3. Quality checks** | `tests/layer3_rules/` | Are the checks themselves right? | Same problem, worse consequence — see §6. |

`conftest.py` and `helpers.py` stay at the root of `tests/`: shared by all three layers,
belonging to none.

**Layer 2 is not redundant with layer 1.** Layer 1 asserts a couple of dtypes; layer 2 is the
only thing checking whole-schema equality *and column order*. Order is load-bearing: AUTO CDC
matches source to target positionally as well as by name, so two columns of the same type
swapping places is a real defect a set comparison waves through.

**Layer 3 splits two ways, and the split is the point:**

- `test_rule_hygiene.py` is **cross-cutting**. It iterates `CHECKS` and checks every check in
  the project for well-formedness, snake_case naming, project-wide name uniqueness and
  error-criticality policy — plus the two-directional wiring check that every dataset in
  `checks/` is one a transformation applies and vice versa. *Nobody edits it to add an entity.*
- `test_<entity>_rules.py` and `test_child_rules.py` are the **behavioural** half: your checks
  pass clean fixtures and fire on the records built to break them. Only you can write that half.

---

## 6. Why layer 3 exists: DQX fails *soft*

This is the concept the whole quality framework is built around.

DQX resolves a check's columns at **apply** time. When it cannot — a typo'd `column:`, a
`filter:` or an `sql_expression` naming something that is not there — **it does not raise**. It
marks the check `skipped=true` and emits that on every row:

| Situation | What you see |
|---|---|
| `criticality: error` | 100% of rows quarantined, the silver table empties — and the pipeline reports a **successful update** |
| under `suppress_skipped` | the check becomes a no-op and reports a clean **100% pass, forever** |

`@dp.expect_all` failed the update loudly instead. DQX does not. And
`DQEngine.validate_checks` cannot see it either — it validates function names, argument names
and shape, none of which need a DataFrame.

`helpers.assert_no_skipped_checks` is what closes that hole, and **every layer-3 behaviour test
routes through it** — including the ones whose point is that checks *do* fire, since a skipped
check is not a fired one.

### Why the child queries live in `shaping/`

The child-table queries (aptitudes, positions, the aptitude bridge, workspace history) used to
be f-strings inline in `transformations/silver/`, unreachable from pytest. Their checks were
therefore only ever handed to `F.expr()` — which parses a predicate but **resolves no column
names**. A check naming `proficiency_level` where the column is `proficiency` passed the entire
suite. Returning the SQL from a plain function, with no `pyspark.pipelines` import, is what
lets a test register a fixture as a view, run the real query, and resolve the real checks
against real columns. One module per query, named for the table it produces.

Each query takes the source relation as an argument, so the pipeline passes
`STREAM(<bronze table>)` and a test passes a temp view name — same text, both places.

### The two limits of `assert_no_skipped_checks`

Both are measured, and both make it pass on a check it should have caught:

1. **Zero rows.** DQX decides to skip at *analysis* time but reports it *per row* —
   `_build_result_struct(..., skipped=True)` is a Column expression. A result with no rows
   carries no marker. The helper now **rejects an empty result outright** rather than returning
   clean, and `test_assert_no_skipped_checks_rejects_an_empty_result` pins that. Where a fixture
   legitimately yields no rows, the test says so explicitly — see `KNOWN_EMPTY` in
   `tests/layer3_rules/test_child_rules.py`.
2. **`suppress_skipped`.** Under `ExtraParams(suppress_skipped=True)` DQX emits no marker at all
   and this assertion cannot see the skip by any route. It works *because* `conftest.py` leaves
   that flag off. Do not turn it on without replacing the assertion with something else.

### What the loader catches, and what it does not

`checks.py` runs `DQEngine.validate_checks` at **import**, so a malformed check fails a pipeline
update and a pytest run identically. Measured against the installed DQX:

| Mistake | Caught by |
|---|---|
| Unknown function, bad argument name, non-dict `check:`, missing `function:`, no arguments at all | **DQX**, at import |
| Missing `criticality:` — DQX silently defaults to `error`, promoting a warning into a withheld row | **the loader / hygiene tests** (DQX does not mind) |
| Missing or non-snake_case `name:` — DQX generates one, silently renaming a metric and breaking an expected-count dict | **the loader / hygiene tests** (DQX never inspects `name`) |
| A name reused across two datasets — reads as one metric, so you debug the wrong table | **the loader** (DQX has no cross-file concept) |
| **A column that does not exist** | **only** a behaviour test, via `assert_no_skipped_checks` |

---

## 7. Criticality is a contract, not a label

```yaml
- name: profile_id_not_null
  criticality: error      # row WITHHELD from silver, written to quarantine instead
- name: completion_rate_is_fraction
  criticality: warn       # row in BOTH silver and quarantine
```

`error` is only ever for a row that cannot be used at all — in practice, a null primary key.
`test_error_criticality_checks_only_ever_guard_a_key` enforces that by inspecting the check
*function*, because withholding rows is invisible: the pipeline reports a healthy run and the
data is simply not there.

> **The quarantine table is not a dead-letter queue.** It is "every row anything fired on", and
> a `warn` row is in it *and* in silver. Query `WHERE _errors IS NOT NULL` to see only what was
> actually withheld. The README has the full set of quarantine queries.

**Prefer a built-in check function to `sql_expression`.** Built-ins carry typed messages and
tested edge handling, and `is_in_range` in particular is null-safe by construction
(`(col < min) | (col > max)`), so the `x IS NULL OR …` wrapper is not needed. Reach for
`sql_expression` only for things no built-in covers — two-column comparisons, parse checks — and
give it a `msg:`, because DQX has no typed message to fall back on there.

**Null semantics differ from the `expect_all` decorators this replaced**, and the difference is
load-bearing. DQX flags a row when `NOT(<expression>)` is `TRUE`, so a **NULL expression flags
nothing**; `expect_all` flagged anything that was not `TRUE`, NULL included. That is why several
checks carry an explicit `IS NOT NULL` guard: it preserves the row counts of the predicate they
replaced. Do not "simplify" those away.

---

## 8. Verification, in order

```bash
uv run pytest                                   # all three layers; no workspace needed
uv run ruff check . && uv run ruff format --check .
databricks bundle validate                      # yml and references only
databricks bundle deploy                        # to your personal `local` target
databricks bundle run whoz_ingestion_etl
```

The first two commands are the **whole** verification the `[TESTS]` track needs. Everything
from `bundle validate` down is the **only** verification steps 8–10 get: a deploy and a real run
are not optional polish here, they are the test.

On that first run, three things need human eyes and nothing else can check them:

1. **Count the datasets in the update's graph** against the files in `transformations/`.
   `libraries.glob`'s `transformations/**` has to recurse into `bronze/` and `silver/`, and the
   match happens server-side at update time. The symptom of a non-recursive match is not an
   error — it is a graph missing tables.
2. **Confirm the `_checked` views attached and the quarantine tables exist.** Nothing local
   proves that `spark.readStream.table("<view>")` resolves a temporary view in the pipeline
   graph, that `WorkspaceClient()` authenticates on serverless, or that a quarantine table
   infers a usable schema from DQX's result structs.
3. **Query quarantine and look for `skipped` *first*.** The pipeline's Data Quality tab does
   **not** populate for DQX-checked datasets — DQX does not use Expectations — so quarantine is
   the only place findings live now.

```sql
SELECT r.name, r.message, r.skipped, count(*) AS rows
FROM silver.<table>_quarantine
LATERAL VIEW explode(concat(coalesce(_errors, array()), coalesce(_warnings, array()))) AS r
GROUP BY r.name, r.message, r.skipped ORDER BY rows DESC;
```

Any row with `skipped = true` means DQX could not resolve that check's columns and enforced
nothing (or quarantined everything). A `warn` check firing broadly is information: either the
source differs from what you analysed, or the check is wrong. Both are worth knowing before
promoting to `dev`.

Also worth knowing: constructing `DQEngine(WorkspaceClient(), spark=spark)` is a **live
workspace call** — `_verify_workspace_client` runs `ws.clusters.select_spark_version()`
unconditionally, twice per engine, so the three transformation modules make six control-plane
round trips before a single row is read. Nothing in pytest exercises that path (`conftest.py`
passes a `MagicMock`), so it first runs at deploy.

---

## 9. The blind spots, stated plainly

Most rows in the runbook's "what fails if you skip a step" table name a test that catches the
mistake locally. These four do not:

| If you skip | What fails |
|---|---|
| Matching `BRONZE_KEYS` to bronze's `try_variant_get` paths | **Nothing, anywhere.** Tests pass, production is wrong. Check by eye. |
| A correct `FILE_NAME_GLOB` | **Nothing.** Auto Loader finds no files and reports a healthy run. |
| The three-object wiring being *correct*, not merely present | **Nothing locally.** The first deploy is the only test. |
| Writing the layer-2 and layer-3 test files at all | **Nothing, ever.** Hygiene still passes on your checks; a per-entity test file that does not exist fails nothing. |

It is no coincidence that the first three belong to **steps 8 and 9**: the `[TESTS]` track can
only reach as far as the `whoz_ingestion/` seam, so the one place a mistake survives to
production is the `[PIPELINE]`-only files it cannot execute.

The fourth is different in kind and worth sitting with. One-file-per-entity-per-layer makes the
gap **visible** — an empty slot next to `test_talent_contract.py` — but it does not make it
*enforced*. Those files get read by a human or they get read by nobody.

### Known gap today

The **profile and talent** child datasets have no `violations.json` records, so ten checks are
applied but never made to fire. The `user` entity closed this for its own child table —
`fixtures/whoz_users/violations.json` trips all three `whoz_user_workspace_roles` checks, and
`test_every_user_child_rule_is_covered_by_the_violations_fixture` keeps it closed. The other
four child datasets still need the same treatment; that test is the shape to copy.

Separately, `end_date_parsed` does not fire on the record it was written for: Spark 4.0 parses
`+22015-07-31` as a valid `DATE` rather than returning NULL, contradicting
`whoz_profile_data_model.md` §2. `test_extended_year_end_date_parses_rather_than_nulling` pins
that measurement, so the day the behaviour changes — or Databricks Runtime turns out to
differ — it is a visible failure and not a surprise.

---

## 10. Quick reference

| I want to… | Go to |
|---|---|
| Change how a field is parsed | `src/whoz_ingestion/shaping/<entity>.py`, then layer 1 |
| Add or change a column | `schemas/<entity>.yml` **and** the `select()` in `shaping/<entity>.py` — layer 2 fails until they agree |
| Add a quality check | `checks/<dataset>.yml` + a record in `fixtures/<entity>/violations.json` that trips it |
| Change a child-table query | `src/whoz_ingestion/shaping/<entity>_<collection>.py` (never inline it in `transformations/`) |
| Add a table to an existing entity | a new `transformations/silver/<table>.py` + a new `shaping/<table>.py` — never a second dataset in an existing file |
| Add a whole new export file | [`adding_a_source_entity.md`](adding_a_source_entity.md), from step 0 |
| Understand a type hazard | [`whoz_profile_data_model.md`](whoz_profile_data_model.md) |
| Find out why a row is missing from silver | `silver.<table>_quarantine WHERE _errors IS NOT NULL` |
| Find out whether a check is actually enforcing anything | the same table, `WHERE r.skipped` |

**The one-sentence version:** put the logic in `whoz_ingestion/` where tests can reach it, keep
`transformations/` trivial, express schemas and checks as data so a loader can validate them at
import, and remember that every failure mode this framework guards against is a *silent* one —
which is why the tests assert that checks ran, not merely that they passed.

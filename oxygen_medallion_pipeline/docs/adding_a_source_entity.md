# Adding a source entity

A step-by-step checklist for ingesting a new export file into the medallion pipeline —
what to create, what to change, and in what order.

**Scope.** This document covers a new **entity**: a new export file landing in the existing
`landing.source` volume, modelled by the existing `whoz_ingestion_etl` pipeline. A whole new
**source system** (new vendor, new pipeline, new bundle resource) needs everything here plus
the four extra items in the last section.

## Where things live, and why

Read this once; the rest of the document assumes it.

```
src/whoz_ingestion_etl/     # the pipeline: what Lakeflow loads and runs
  transformations/          # imports pyspark.pipelines — cannot be unit tested
    bronze/  <entity>.py
    silver/  <entity>.py
src/whoz_ingestion/         # the shared package: plain DataFrames, what tests can reach
  shaping/ <entity>.py      # shape_<entity>() + its <ENTITY>_COLUMNS DDL
  expectations.py           # every quality rule in the project, as data
tests/
  conftest.py  helpers.py         # shared by all three layers, owned by none
  layer1_shaping/   test_<entity>_shaping.py
  layer2_contract/  test_<entity>_contract.py
  layer3_rules/     test_rule_hygiene.py          # cross-cutting, never edited per entity
                    test_<entity>_rules.py        # behavioural, one per entity
```

**`whoz_ingestion_etl/` vs `whoz_ingestion/` is a testability seam, not a tidiness one.** It
is the thesis of this whole document: `pyspark.pipelines` only fully exists inside a running
Lakeflow pipeline, so anything that imports it is unreachable from pytest. Everything worth
testing therefore lives on the `whoz_ingestion/` side of the line, and the
`whoz_ingestion_etl/` side is kept as close to zero logic as it can be. Steps 3 and 4 are the
real work; steps 8 and 9 are wiring.

They are **sibling folders under `src/`, not nested**, and that is deliberate: the shared
side has two consumers (the pipeline and the test suite), so it should not read as though it
belongs to the pipeline. The pipeline's `root_path` is `src/`, which is what puts
`whoz_ingestion` on `sys.path` at runtime under the same name `pyproject.toml`'s
`pythonpath` gives it in pytest — one import prefix, `whoz_ingestion.x`, in both places.

**`transformations/` is layer-first (`bronze/`, `silver/`), not entity-first.** A future
`gold/` layer joins *across* entities, so it needs a peer folder; in an entity-first tree
(`whoz_profile/bronze.py`, `whoz_profile/silver.py`) a cross-entity gold table has nowhere
to live without a folder that contradicts the scheme.

**`tests/` mirrors that: a folder per layer, the entity in the filename.** Every layer then
has the same shape, and adding an entity is "copy three files" rather than "create one file
and edit two shared ones". The filenames are deliberately unique across folders — there are
no `__init__.py` files under `tests/`, so two `test_profile.py` in different folders would
trip pytest's "import file mismatch". Keep the names distinct; do not add `__init__.py`.
`conftest.py` and `helpers.py` stay at the `tests/` root, and `pyproject.toml`'s
`pythonpath` names `tests` so `from helpers import ...` keeps resolving from a subfolder.

**`whoz_ingestion/expectations.py` stays one file on purpose.** It is the one shared file you
still edit (step 4), and splitting it per entity would cost more than it saves: a rule set
is ~10 lines of *data* per entity against ~160 lines of *code* for a shaping module; the
file holds two genuinely cross-entity things — the `ALL_RULE_SETS` registry and the
globally-unique-rule-name convention, where one rule's name is justified by a comment about
another rule three lines above; and it already carries rules for four child datasets
(aptitudes, positions, aptitude refs, workspace history) that are not top-level entities at
all and would have no per-entity file to go in. For the same reason a `*_COLUMNS` DDL string
stays next to its `shape_*()` function: "column order must match `select()` exactly" is only
checkable by eye when the two are adjacent.

## Two tracks

Every step below is tagged. **`[PIPELINE]`** steps are code that ships and runs in
Databricks. **`[TESTS]`** steps are the local safety net — they never leave your laptop and
CI. They interleave rather than running in two blocks, because each test step covers the
pipeline step immediately before it, and writing them in that order is what keeps the
feedback loop short.

| Step | File | New or changed | Track |
|---|---|---|---|
| 0 | `docs/<source>_<entity>_data_model.md` | new | analysis |
| 1 | `fixtures/<entity>/{typical,hazards,violations}.json` | **new** (3 files) | `[TESTS]` |
| 2 | `tests/conftest.py` | changed (3 edits) | `[TESTS]` |
| 3 | `src/whoz_ingestion/shaping/<entity>.py` | **new** | `[PIPELINE]` |
| 4 | `src/whoz_ingestion/expectations.py` | changed | `[PIPELINE]` |
| 5 | `tests/layer1_shaping/test_<entity>_shaping.py` | **new** | `[TESTS]` |
| 6 | `tests/layer2_contract/test_<entity>_contract.py` | **new** (3 tests) | `[TESTS]` |
| 7 | `tests/layer3_rules/test_<entity>_rules.py` | **new** (3 tests) | `[TESTS]` |
| 8 | `src/whoz_ingestion_etl/transformations/bronze/<entity>.py` | **new** | `[PIPELINE]` |
| 9 | `src/whoz_ingestion_etl/transformations/silver/<entity>.py` | **new** | `[PIPELINE]` |
| 10 | `resources/whoz_ingestion_etl.pipeline.yml` | changed | `[PIPELINE]` |
| 12 | `README.md`, `src/whoz_ingestion_etl/README.md` | changed | docs |

Six new source files — three that ship to Databricks (steps 3, 8, 9) and three tests (steps
5, 6, 7) — plus the three fixtures and the data model doc. Exactly **two** existing Python
files change: `tests/conftest.py` and `whoz_ingestion/expectations.py`.

**All three test steps are new files, and that is the recent improvement.** Layers 2 and 3
used to be "append your three tests to the shared `test_schema_contract.py` /
`test_expectations.py`". They are now one file per entity per layer, which buys two things.
There is no shared test file for two people adding two entities to conflict on. And there is
nothing to *forget* appending to: a missing `tests/layer2_contract/test_<entity>_contract.py`
is a visibly absent file next to `test_talent_contract.py`, whereas a missing block inside a
600-line shared module is invisible and the suite stays green without it. Practically, steps
5, 6 and 7 are "copy the three talent files, s/talent/<entity>/, fix the expected values".

**Step 4 is pipeline code, not test code**, even though most of what the step says is about
tests. `whoz_ingestion/expectations.py` is imported by the transformations — the rules genuinely
run in Databricks. It is read by *both* sides: the pipeline imports the individual
`*_MUST_HOLD` / `*_SHOULD_HOLD` dicts and applies them via decorators, while the test suite
imports `ALL_RULE_SETS`, a registry that exists only so `tests/layer3_rules/test_rule_hygiene.py`
can iterate every rule in the project. That dual readership is the entire reason the rules
are data in a module instead of string literals in a decorator argument.

**Why there are more `[TESTS]` steps than `[PIPELINE]` ones.** Not because testing is the
bigger job — because of where the seam is. `transformations/**/*.py` are deliberately thin
wrappers: read the upstream table, call a `whoz_ingestion/` function, return it. They can't be
unit tested at all (`pyspark.pipelines` only fully exists inside a running Lakeflow pipeline,
and the module-level `spark.conf.get` calls fail without one), so all the logic worth testing
was pushed into `whoz_ingestion/`, which takes and returns plain DataFrames. Steps 3 and 4 are
where the real work happens; steps 8 and 9 are mostly wiring and configuration, and they are
proved by deploying, not by pytest.

**Local vs. workspace.** Steps 1–9 are pure local work — no Databricks, no credentials,
`uv run pytest` is the whole feedback loop, and that includes *writing* the transformation
files. Only steps 10–11 need a workspace. Do not start at step 8 because it looks like the
real work; the shaping function is the real work, and it is testable long before any of it
is deployed.

---

## 0. Analyse the export before writing any code  ·  *no code yet*

Nothing here produces a file, but every decision below is downstream of it. Get a real
export file and answer:

- [ ] **Root shape** — a pretty-printed JSON *array*, JSONL, or a single object? Both Whoz
      exports are arrays, which is why bronze needs `multiLine` + `singleVariantColumn` +
      an explicit `variant_explode`. A JSONL source would not.
- [ ] **Grain** — one row per *what*? This becomes the AUTO CDC `keys=[...]` and the table
      comment.
- [ ] **Primary key** — which field is stable and non-null? It becomes the `*_MUST_HOLD`
      drop rule, and `NOT NULL` in the DDL.
- [ ] **Foreign keys** — which fields join to already-modelled tables (e.g. `profile_id` →
      `silver.whoz_profiles`)?
- [ ] **Sequencing field** — the source's own last-modified timestamp. AUTO CDC sequences
      by this, *not* by ingest time, so a late-landing backfill doesn't overwrite newer data.
      If the export has no such field, stop and decide what to do — SCD is not safe without one.
- [ ] **Polymorphic fields** — anything that arrives as int on some records and float on
      others, or object on some and array on others. Each one is a `hazards.json` record and
      probably a `*_SHOULD_HOLD` rule.
- [ ] **Timestamp precisions** — collect the distinct formats present (second, millisecond,
      nanosecond). Cast to `timestamp` rather than parsing with a format string; a fixed
      format handles at most one of them.
- [ ] **Nested arrays** — each one is either a `*_count` column, a child table, or ignored.
      Decide which, per array.
- [ ] **What you deliberately will not model** — write this down. See
      `whoz_ingestion/shaping/talent.py`'s header for the standard to match: it records that the
      embedded `profile` object is *not* re-modelled, why, and how to reverse it.
- [ ] Capture the analysis in `docs/<source>_<entity>_data_model.md`, alongside
      `docs/whoz_profile_data_model.md`.

---

## 1. `[TESTS]` Fixtures — `fixtures/<entity>/`

Three files, and the names are a contract the tests rely on. Each is a JSON **array** of
source objects in exactly the shape the vendor exports, so a fixture can be a trimmed copy
of real records with no reformatting.

### 1a. `typical.json` — the happy path

- [ ] 2–3 ordinary records, copied from the real export and trimmed.
- [ ] Keep realistic ids (24-hex ObjectIds for Whoz) — these are the records that stand in
      for production data.
- [ ] Cover both ends of what "ordinary" means: the talent fixture has one near-empty record
      and one with a real headline, two workspace-history entries and an object-form
      `completionDetails`.
- [ ] Anonymise. Fixtures are committed to git; bronze payloads are not fully anonymised in
      production for a reason.
- [ ] **Every quality rule must pass on this file.**

### 1b. `hazards.json` — awkward but legitimate

One record per documented type hazard from step 0. Awkward is not invalid — **every rule
must pass here too**, and a rule that fires on this file is a false positive.

- [ ] Use descriptive ids, not ObjectIds: `hazard-timestamp-precisions`,
      `hazard-empty-collections`. Assertions then read as the thing being tested.
- [ ] One record per hazard, named for it. The talent fixture's seven:
      `hazard-headline-all-null`, `hazard-empty-collections`, `hazard-collections-absent`,
      `hazard-timestamp-precisions`, `hazard-completion-rate-int`,
      `hazard-multiple-workspace-history`, `hazard-sparse-record`.
- [ ] Cover at minimum: absent key vs. present-but-null vs. present-with-all-fields-null;
      an empty array vs. an absent array (they produce `0` and `NULL` respectively, which
      downstream aggregates treat differently); every timestamp precision; a record with
      almost nothing but the key set.
- [ ] Include a **sparse record** — this is the fixture layer 2 runs against, because every
      column being NULL still carries a type, so it compares the DDL against shaping at its
      least informative.

### 1c. `violations.json` — designed to break the rules

- [ ] **One record per quality rule**, so expected counts are all `1` and a failure names
      exactly one rule. Deviate only deliberately: the talent fixture's
      `violation-profile-as-array` trips two rules, and the test comment explains why that
      pair firing together is the point.
- [ ] Descriptive ids again (`violation-completion-rate-as-percentage`), except for the
      record violating the null-key rule — that one's id *is* `null`.
- [ ] Each record should violate its rule **and nothing else**, because
      `assert_violations` treats the expected-counts dict as complete: a record that also
      trips an unrelated rule fails the test.

---

## 2. `[TESTS]` Wire the fixtures into `tests/conftest.py`

Three edits, all in one file. This is the only entity-specific code in the whole shared test
scaffolding.

- [ ] **`BRONZE_KEYS`** — add `"<bronze_table>": {column: json_path, ...}` listing the
      identity columns bronze lifts out of the payload. These **must match the
      `try_variant_get` calls in `transformations/bronze/<entity>.py`** exactly. A mismatch
      means the tests build input by a different route than production reads it, so they can
      pass while production is broken.
- [ ] **`SOURCE_FILES`** — add one entry, a `dbfs:/Volumes/...` path mirroring the real
      `FILE_NAME_GLOB`. Nothing asserts on it today; it exists so the lineage column looks
      like the file the entity really lands under.
- [ ] **Two fixtures at the bottom**, copied verbatim from the talent pair:
      ```python
      @pytest.fixture()
      def <entity>_bronze(bronze_of):
          return lambda records: bronze_of(records, "<bronze_table>")

      @pytest.fixture()
      def <entity>_fixture(source_records, <entity>_bronze):
          return lambda name: <entity>_bronze(source_records("<bronze_table>", name))
      ```

**Do not** touch `bronze_of` itself, and **never construct a `VARIANT` column by hand.** It
is the one place in the suite that builds one, because getting it wrong is easy and the
failure is baffling — round-trip a VARIANT through a local `Row` and Spark loses the logical
type, re-infers `STRUCT<metadata: BINARY, value: BINARY>`, and `try_variant_get` fails with
`DATATYPE_MISMATCH` on perfectly good JSON.

---

## 3. `[PIPELINE]` `whoz_ingestion/shaping/<entity>.py` — the actual work

The seam that makes everything testable. Model it on `whoz_ingestion/shaping/talent.py`.

- [ ] **No `pyspark.pipelines` import. Ever.** That module only fully exists inside a running
      Lakeflow pipeline. Plain `DataFrame` in, plain `DataFrame` out is what lets a local
      SparkSession test it.
- [ ] **Header comment** stating what the module models and — explicitly — what it
      deliberately does not, and why.
- [ ] **`<ENTITY>_COLUMNS`** — the DDL string handed to `create_streaming_table(schema=...)`.
  - [ ] Column order must match `shape_<entity>()`'s `select()` **exactly**. Order is part of
        the contract: AUTO CDC matches source to target positionally as well as by name.
  - [ ] `NOT NULL` only on the key the `*_MUST_HOLD` rule guards.
  - [ ] Double every apostrophe inside a `COMMENT` (`'Whoz''s'`). An unescaped one ends the
        string literal early and silently takes the whole schema down.
  - [ ] Comments should carry the analysis: units, observed distributions, which rule
        enforces the column, which table a foreign key points at.
- [ ] **`<ENTITY>_HISTORY_COLUMNS`** (only if the entity gets an SCD2 table) — built here as
      `<ENTITY>_COLUMNS + ",__START_AT TIMESTAMP ..., __END_AT TIMESTAMP ..."`, not
      concatenated at the call site, so the tests check the real string the pipeline uses.
      Both must be `TIMESTAMP` to match `sequence_by`.
- [ ] **`shape_<entity>(bronze: DataFrame) -> DataFrame`**
  - [ ] Read every payload field with `try_variant_get` (the local `vg()` helper) — it
        returns NULL on a missing path or bad cast rather than raising, so one malformed
        record doesn't kill the batch.
  - [ ] Take identity columns from the bronze columns, not by re-reading the payload.
  - [ ] Cast timestamps with target type `timestamp`, never a format string.
  - [ ] Array sizes via the `size_of()` helper — and know that it yields `NULL` for an absent
        key and `0` for an empty array.
  - [ ] **Do not emit the `payload` column.** AUTO CDC compares whole rows with `<=>`, and
        VARIANT doesn't support that comparison (`INVALID_ORDERING_TYPE`). The raw JSON stays
        queryable in bronze by key.
  - [ ] Add a **drift sensor column** for any structural assumption you're making, the way
        `profile_container_type` records whether the nested profile is still an `OBJECT`.
        A silent NULLing is only catchable if something observes the shape.

---

## 4. `[PIPELINE]` `whoz_ingestion/expectations.py` — the quality rules

- [ ] **`<ENTITY>_MUST_HOLD`** → `@dp.expect_all_or_drop`, rows are **dropped**. In practice
      this holds exactly one rule: the null primary key check. Anything else is rejected by
      `test_drop_rules_only_ever_guard_a_key`, which requires `IS NOT NULL` in the predicate.
- [ ] **`<ENTITY>_SHOULD_HOLD`** → `@dp.expect_all`, rows are **kept and counted**.
      Everything else lives here: source assumptions that should raise an eyebrow, not delete
      data, if they stop holding.
- [ ] **Write `SHOULD_HOLD` predicates null-safe** — `"x IS NULL OR <check on x>"`. A NULL
      predicate result is not TRUE and therefore counts as a failure, so a range check that
      isn't null-safe flags every row with a missing optional field and the signal drowns.
      Break the rule only where a NULL genuinely *is* the thing you want to hear about.
- [ ] **Rule names must be unique across the whole project** — enforced by
      `test_rule_names_are_unique_across_the_project`, because names surface as metric labels
      in the quality dashboard where two identical names on different tables read as one.
      This is why the talent key rule is `talent_pk_not_null`, not `talent_id_not_null`.
- [ ] **snake_case names**, enforced by `test_rule_is_well_formed`.
- [ ] **Register both dicts in `ALL_RULE_SETS`**, keyed `"<dataset>.must_hold"` /
      `"<dataset>.should_hold"`. Enforced by `test_every_rule_set_is_registered` — the
      registry is what makes `test_rule_hygiene.py` cover your rules with no edit to it.
- [ ] Every registered rule set must be **applied by a decorator** in `transformations/`
      (step 9). Enforced by `test_every_rule_set_is_applied_by_a_transformation`: registering
      a rule set the pipeline never uses is otherwise green everywhere and does nothing.
      (That test walks `transformations/` recursively, so `bronze/` and `silver/` are both
      in scope.)
- [ ] Comment each rule with *what it would mean if it fired*. That sentence is the entire
      value of the rule at 3am.

---

## 5. `[TESTS]` Layer 1 — `tests/layer1_shaping/test_<entity>_shaping.py`

*Does the transformation code do what we intended?* A new file next to
`tests/layer1_shaping/test_talent_shaping.py`; model it on that one.

- [ ] Module docstring opening `"""Layer 1 — ... (<entity> entity)"""`.
- [ ] A `by_<key>(df) -> dict` helper returning `{key: Row}`, so a test can address one
      fixture record by name.
- [ ] **Happy path**, against `typical.json`:
  - [ ] Record count is what you expect.
  - [ ] One test asserting every lifted column on a known record, by value.
  - [ ] A second test covering the populated variant, if `typical.json` has two ends.
  - [ ] `test_empty_input_produces_no_rows_not_an_error` — pass `<entity>_bronze([])`.
        The empty case takes a different code path in `bronze_of` and has broken before.
- [ ] **Hazards**, against `hazards.json` — one test per hazard record, named for the
      behaviour it pins, not the record:
  - [ ] Type normalisation (int and float both landing as `double`), asserting the dtype too.
  - [ ] Every timestamp precision parsing — and put the DataFrame through
        `helpers.to_utc_strings()` first. **Never assert on a collected timestamp:**
        `df.collect()` converts using the *JVM default* zone, which conftest's UTC session
        setting does not reach, so the assertion passes in Madrid and fails on CI's UTC runners.
  - [ ] Empty vs. absent collections producing `0` vs. `NULL`.
  - [ ] The sparse record nulling columns instead of failing.
- [ ] **Structural hazards**, against `violations.json` — pin what *actually happens today*,
      not what you'd like to happen. The array-shaped-profile test is the model: it asserts
      the columns really do go silently NULL, and that the drift sensor sees it.
- [ ] `test_payload_is_not_carried_into_the_output` — asserts `"payload" not in .columns`.

---

## 6. `[TESTS]` Layer 2 — `tests/layer2_contract/test_<entity>_contract.py`

*Does the declared schema still describe what the code produces?* A **new file**, three
tests, copied from `test_talent_contract.py`. Everything is done by `helpers`, so the copy is
close to mechanical: change the imports, the fixture name and the three test names.

- [ ] `test_<entity>_columns_is_valid_ddl` — `assert len(ddl_columns(<ENTITY>_COLUMNS)) > 0`.
      Catches the unescaped-apostrophe class of bug that nothing else in the toolchain
      notices.
- [ ] `test_declared_schema_matches_shape_<entity>_output` — `assert_schema_matches_ddl(
      shape_<entity>(<entity>_fixture("hazards")), <ENTITY>_COLUMNS)`. Use the **hazards**
      fixture, deliberately: all-NULL columns still carry types, which is where an accidental
      type change is most likely to slip through.
- [ ] `test_<entity>_history_schema_adds_only_the_scd2_columns` — the SCD2 columns are
      *appended*, not interleaved, and `sequence_by`'s column is `timestamp`. Skip only if the
      entity has no SCD2 table. Keep the entity in the name; the profile test carries it
      (`test_profile_history_schema_adds_only_the_scd2_columns`) for exactly this symmetry.
- [ ] A module docstring opening `"""Layer 2 — ... (<entity> entity)"""`. The long-form
      explanation of *why* this layer exists lives in `test_profile_contract.py`; point at it
      rather than restating it, the way `test_talent_contract.py` does.

---

## 7. `[TESTS]` Layer 3 — `tests/layer3_rules/test_<entity>_rules.py`

*Are the quality rules themselves right?* This layer is two halves, and knowing which half
you are in is the whole point of the folder having three files in it:

- **`test_rule_hygiene.py` — you never touch it.** It iterates `ALL_RULE_SETS` and asserts,
  over *every* rule in the project, that the predicate parses, that the name is
  snake_case and unique project-wide, that `*_MUST_HOLD` only ever guards a key, and that
  every declared rule set is both registered and applied by a decorator. Your rules start
  being covered by all of that the moment step 4's `ALL_RULE_SETS` entries exist. There is
  nothing to add here, and adding something here is a sign you have written a behavioural
  test in the wrong file.
- **`test_<entity>_rules.py` — a new file, three tests, and the half only you can write.**
  Hygiene proves a rule is well-formed; it cannot know what the rule is *supposed to mean*.
  Copy `test_talent_rules.py`.

The three tests, in the new file:

- [ ] `<ENTITY>_RULES = {**<ENTITY>_MUST_HOLD, **<ENTITY>_SHOULD_HOLD}` at the top of the
      module.
- [ ] `test_<entity>_rules_pass_on_valid_records` — parametrized over `["typical", "hazards"]`,
      `assert_no_violations`. A rule firing here is a false positive, and a false positive
      trains everyone to ignore the dashboard.
- [ ] `test_<entity>_rules_catch_the_records_designed_to_break_them` — `assert_violations`
      with the complete expected-count dict. Complete, not a subset: any rule you don't list
      must have zero failures, which is what makes this catch an *over*-broad rule and not
      just an under-broad one.
- [ ] `test_every_<entity>_rule_is_covered_by_the_violations_fixture` — the guard on the test
      above, failing until every rule fires on something.
- [ ] A module docstring opening `"""Layer 3 — ... (<entity> behaviour)"""` that names
      `test_rule_hygiene.py` as the other half, so the next reader lands in the right file.
- [ ] Import only the two rule-set names this entity uses, and `shape_<entity>`.

**Child datasets do not get one of these files.** A rule set for a table whose SQL still
lives inline in `transformations/silver/` — the aptitudes, positions and workspace-history
rules — has no local DataFrame to resolve against, so it gets hygiene checks only. That is a
known gap, not an oversight; see the blind-spot table at the end.

---

## 8. `[PIPELINE]` `transformations/bronze/<entity>.py`

The Auto Loader landing table: the file that defines the table everything above assumes
exists. Copy `transformations/bronze/whoz_talents.py`.

Nothing in the test suite executes this file — it imports `pyspark.pipelines` and calls
`spark.conf.get` at module scope, so it only runs inside a Lakeflow pipeline. Deploying is
what proves it. That makes the two hand-checks below (`BRONZE_KEYS`, `FILE_NAME_GLOB`) the
most important lines in the step; see the blind-spot table at the end.

- [ ] `SOURCE_PATH` / `SCHEMA_PATH` from `spark.conf.get("<source>.<entity>.source_path")`
      etc. — **no fallback default**. A deploy missing the config should fail loudly naming
      the key, not land quietly on someone else's volume.
- [ ] `FILE_NAME_GLOB` matching this export's filename, tolerant of a `YYYY-MM-DD_` prefix.
      A wrong glob makes Auto Loader find nothing rather than fail — verify it against the
      first real file that lands.
- [ ] `CATALOG` / `BRONZE_SCHEMA` from config; build `BRONZE_TABLE` as an f-string. Never
      hardcode a table name.
- [ ] `@dp.table(name=..., comment=..., table_properties={"quality": "bronze",
      "delta.enableChangeDataFeed": "true"}, cluster_by=["ingest_date", "<key>"])`.
- [ ] Auto Loader read: `format("cloudFiles")`, `cloudFiles.format=json`, `multiLine=true`,
      `singleVariantColumn=payload`, `cloudFiles.schemaLocation=SCHEMA_PATH`,
      `pathGlobFilter=FILE_NAME_GLOB`.
- [ ] Select `_metadata.*` into plain columns **before** the temp view — the hidden struct
      does not survive `createOrReplaceTempView`.
- [ ] `raw.createOrReplaceTempView(...)` then `spark.sql(...)`. This looks redundant and is
      not: binding a DataFrame via `spark.sql(..., raw=raw)` fails inside a real pipeline with
      `_dlt_sql_fn() got an unexpected keyword argument`, and *only* there — local pytest and
      `databricks bundle validate` both pass happily.
- [ ] `LATERAL variant_explode(b.payload)` to get one row per array element (array-root
      exports only).
- [ ] Lift the identity columns with `try_variant_get` — **the same paths as `BRONZE_KEYS`**
      in conftest.
- [ ] Standard lineage columns: `source_file`, `source_file_name`, `source_file_size`,
      `source_file_modified_at`, `ingested_at`, `ingest_date`, `source_system`,
      `source_entity`.
- [ ] Drift sensors: `payload_top_level_keys`, plus any container-type sensor the entity
      needs.
- [ ] A `<bronze_table>_payload_shapes` `@dp.materialized_view` grouping the drift sensors —
      a materialized view, so it never blocks ingestion.

---

## 9. `[PIPELINE]` `transformations/silver/<entity>.py`

The wiring that turns step 3's shaping function and step 4's rules into real tables. It
should contain no logic of its own — if you find yourself writing a transformation here,
it belongs in `whoz_ingestion/shaping/` where it can be tested. Untested locally for the same
reason as step 8.

- [ ] Import from `whoz_ingestion.shaping.<entity>` and `whoz_ingestion.expectations`. Note the
      import root is `whoz_ingestion.x`, **not** `src.whoz_ingestion.x` or
      `whoz_ingestion_etl.whoz_ingestion.x` — the pipeline's `root_path` *is* `src`, and
      `pyproject.toml`'s `pythonpath` points pytest at the same folder, so a wrong prefix
      fails locally instead of at deploy.
- [ ] A `@dp.temporary_view` named `<entity>_shaped` that returns
      `shape_<entity>(spark.readStream.table(BRONZE_TABLE))`, decorated with
      `@dp.expect_all_or_drop(<ENTITY>_MUST_HOLD)` and `@dp.expect_all(<ENTITY>_SHOULD_HOLD)`.
      Applying the rules on the shaped view means they run once and protect every downstream
      target.
- [ ] `dp.create_streaming_table(schema=<ENTITY>_COLUMNS, table_properties={"quality":
      "silver"}, cluster_by=[...])` for the SCD1 table.
- [ ] `dp.create_auto_cdc_flow(target=..., source="<entity>_shaped", keys=[<pk>],
      sequence_by=F.col("source_last_modified_at"), stored_as_scd_type="1")`.
- [ ] The SCD2 pair, identical but `schema=<ENTITY>_HISTORY_COLUMNS` and
      `stored_as_scd_type="2"`.
- [ ] Child tables (one per nested array you chose to explode) as `@dp.table` functions using
      `LATERAL variant_explode(b.payload:<field>)` over `STREAM(<bronze_table>)`, each with its
      own `*_MUST_HOLD` / `*_SHOULD_HOLD` pair.
- [ ] For any parsed date/number in a child table, keep **both** the parsed value and the raw
      string (`since_date` + `since_raw`), so a value the cast can't handle is visible rather
      than just NULL — and add the matching `*_parsed` warn rule.

---

## 10. `[PIPELINE]` Pipeline configuration — `resources/whoz_ingestion_etl.pipeline.yml`

- [ ] Add `<source>.<entity>.source_path` under `configuration:`.
- [ ] Add `<source>.<entity>.schema_path` — **its own directory**. Two Auto Loader streams
      must never share a checkpoint location.
- [ ] Qualify both with `${var.catalog}`, e.g.
      `/Volumes/${var.catalog}/landing/source/`. The two `whoz.profiles.*` paths are still
      hardcoded to `oxygen_dev` — that is a known open bug, not a pattern to copy: hardcoded,
      test and prod read dev's volume.
- [ ] Nothing else needs to change. `libraries.glob` already includes all of
      `transformations/**`, and `databricks.yml`'s `include: resources/*.yml` already picks up
      the pipeline.

      That glob now has to **recurse** into `bronze/` and `silver/`, which it did not have to
      do when the transformation modules sat flat. Databricks documents `**` as matching
      nested directories, and `databricks bundle validate` passes — but validate does *not*
      prove it. All the CLI does with that glob is rewrite it to a workspace-absolute path;
      the match itself happens server-side when the pipeline is updated. So the first deploy
      is the real check, and it is in the step 11 list for that reason. If it turns out `**`
      does not recurse, replace the one entry with an explicit `bronze/**` + `silver/**` pair.

---

## 11. Verify, in this order  ·  *both tracks*

The first two commands are the whole verification the `[TESTS]` track needs. Everything from
`bundle validate` down is the *only* verification the `[PIPELINE]` track gets — steps 8, 9
and 10 have no local test that can fail, so a deploy and a real run are not optional
polish here, they are the test.

- [ ] `uv run pytest` — all three layers green locally. No workspace needed.
- [ ] `uv run ruff check .` and `uv run ruff format --check .`
- [ ] `databricks bundle validate` — catches yml and reference errors, but note it passes
      green on a broken DDL string or a bad expectation predicate. That's what layers 2 and 3
      are for.
- [ ] `databricks bundle deploy` to your personal `local` target, then
      `databricks bundle run whoz_ingestion_etl` — the first run against real data is the only
      thing that tests Auto Loader's glob, the landing path and the pipeline config.
- [ ] **On that first deploy, confirm the pipeline actually sees every transformation
      module.** `libraries.glob`'s `transformations/**` has to recurse into `bronze/` and
      `silver/`; `bundle validate` cannot tell you whether it does, because the CLI only
      rewrites the glob to a workspace path and the match happens server-side at update
      time. The symptom of a non-recursive match is not an error — it is a pipeline whose
      graph is missing tables. Count the datasets in the update's graph against the files in
      `transformations/`, once, and you never have to wonder again.
- [ ] Check the pipeline's data quality metrics after that first run. A `*_SHOULD_HOLD` rule
      firing broadly on real data is information: either the source is different than you
      analysed, or the rule is wrong. Both are worth knowing before the promotion to `dev`.
- [ ] Query `<bronze_table>_payload_shapes` — one row means one payload shape, which is the
      good case. More than one is the drift monitor doing its job on day one.
- [ ] PR into `dev`. CI validates the target and runs pytest; promote `dev` → `test` → `main`.

---

## 12. Documentation to update

- [ ] `README.md` — a **new `### <Entity>` subsection** under "The Whoz source", with its
      own table of bronze/silver tables. One entity per subsection, never appended to
      another entity's table: the whole point of that split is that a reader looking for
      one export never has to work out which rows belong to it. Also add the entity to
      "What's covered today" and your three new test files to the three-layer table under
      "Testing".
- [ ] `src/whoz_ingestion_etl/README.md` — a bullet per new transformation file, under that
      entity's heading (add the heading if it's a new entity).
- [ ] `docs/<source>_<entity>_data_model.md` — the analysis from step 0.
- [ ] The header comment of every file you created. The convention in this repo is that the
      header explains the *decision*, not the mechanics: what was rejected and why.

---

## What you get for free, and what fails if you skip a step

| If you skip | What fails, and when |
|---|---|
| Registering in `ALL_RULE_SETS` | `test_every_rule_set_is_registered`, locally |
| Applying a rule set in `transformations/` | `test_every_rule_set_is_applied_by_a_transformation`, locally |
| A `violations.json` record for a new rule | `test_every_<entity>_rule_is_covered_by_the_violations_fixture`, locally |
| Writing steps 6 and 7's files at all | Nothing, ever. Hygiene still passes on your rules; a per-entity test file that does not exist fails nothing. One-file-per-entity-per-layer makes that gap **visible** — an empty slot next to `test_talent_contract.py` — it does not make it enforced. This is the row the folder layout improves and does not close. |
| Keeping the DDL in step with the `select()` | `test_declared_schema_matches_shape_<entity>_output`, locally |
| Doubling an apostrophe in a `COMMENT` | `test_<entity>_columns_is_valid_ddl`, locally |
| A non-key predicate in `*_MUST_HOLD` | `test_drop_rules_only_ever_guard_a_key`, locally |
| A duplicate rule name | `test_rule_names_are_unique_across_the_project`, locally |
| A typo'd column in a rule predicate | `test_<entity>_rules.py`, locally — **but only for entities whose shaping lives in `whoz_ingestion/shaping/`**. Child tables built inline in `transformations/silver/` get `test_rule_hygiene.py` only, and a bad column there surfaces at pipeline update. |
| `to_utc_strings()` on a timestamp assertion | Nothing locally; CI, on a UTC runner |
| Matching `BRONZE_KEYS` to the bronze `try_variant_get` paths | Nothing, anywhere. The tests keep passing and production is wrong. Check this one by eye. |
| A correct `FILE_NAME_GLOB` | Nothing. Auto Loader finds no files and reports a healthy run. |

The last two are the framework's real blind spots, and it is no coincidence that both belong
to **step 8**: the `[TESTS]` track can only reach as far as the `whoz_ingestion/` seam, so the
one place a mistake survives to production is the `[PIPELINE]`-only files it can't execute.
Everything above those two rows is caught before merge — except the "steps 6 and 7" row,
which is caught by review rather than by pytest. Those get read by a human or they get read
by nobody.

---

## If it's a whole new source system, not just an entity

Everything above, plus:

- [ ] `src/<source>_etl/` alongside `whoz_ingestion_etl/`, holding only `transformations/`
      (same `bronze/` + `silver/` layer-first split), **plus** a sibling `src/<source>/`
      shared package alongside `whoz_ingestion/` (`shaping/` plus one `expectations.py`).
      One pair per source, never one shared package for two sources: a rename or a rule
      change in one vendor's data model should not be able to break another's. Keep the
      shape identical — the layout is the part of this document that generalises, and a
      second source that arranges itself differently makes both harder to read.
- [ ] `resources/<source>.pipeline.yml` — its own `root_path`, `libraries.glob` and
      `configuration` block. Picked up automatically by `databricks.yml`'s
      `include: resources/*.yml`; no new bundle and no change to `databricks.yml`.
- [ ] `resources/<source>_refresh.job.yml` — copy the Whoz one, pointing at the new pipeline
      id.
- [ ] Decide whether the new source shares `tests/conftest.py` or gets its own. Sharing is
      fine while `bronze_of`'s VARIANT-payload assumption holds; a source that lands CSV or
      Parquet needs its own builder.
- [ ] Catalog grants for `sp-oxygen-cicd` if the new pipeline writes anywhere the existing
      grants don't already cover.

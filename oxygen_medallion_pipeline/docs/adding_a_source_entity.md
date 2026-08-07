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
    silver/  <table>.py     # ONE FILE PER TABLE, named for the table it produces
src/whoz_ingestion/         # the shared package: plain DataFrames, what tests can reach
  schemas/ <entity>.yml     # that table's columns, in order, as data
  contract.py               # loads a schema file, renders the DDL string
  shaping/ <entity>.py      # shape_<entity>() + its <ENTITY>_COLUMNS, rendered
  shaping/ <table>.py       # one module per exploded child table, as importable SQL text,
                            #   prefixed with the entity that owns it
  checks/  <dataset>.yml    # one dataset's quality checks, as a native DQX check list
  checks.py                 # loads and validates them -> CHECKS
  dq.py                     # the pipeline's one DQEngine, built lazily on first use
tests/
  conftest.py  helpers.py         # shared by all three layers, owned by none
  layer1_shaping/   test_<entity>_shaping.py
  layer2_contract/  test_<entity>_contract.py
  layer3_rules/     test_rule_hygiene.py          # cross-cutting, never edited per entity
                    test_<entity>_rules.py        # behavioural, one per entity
                    test_child_rules.py           # behavioural, the exploded datasets
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

**`whoz_ingestion/checks/` is one file per dataset, not per entity, and not one shared file.**
Per *dataset* because that is the unit DQX applies: a file is exactly the flat check list
`apply_checks_by_metadata` takes, so `CHECKS["<dataset>"]` is the file, unmodified, and
`FileChecksStorageConfig` could load it as-is if the checks ever move to a volume or a Delta
table. Not one shared file, because four of the six datasets are child tables (aptitudes,
positions, aptitude refs, workspace history) rather than top-level entities, and a single
file would make "which checks does this dataset have" a matter of scrolling. The one
cross-entity convention that survives the split — globally unique check names — is enforced
by `checks.py` at import and again by `test_loader_rejects_a_name_reused_across_two_datasets`, so it
does not need a shared file to live in. Schemas work the same way, one per entity in
`schemas/`, because a schema *is* per entity and "column order must match `select()` exactly"
is a property of one table.

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
| 3 | `src/whoz_ingestion/schemas/<entity>.yml` + `shaping/<entity>.py` | **new** (2 files) | `[PIPELINE]` |
| 4 | `src/whoz_ingestion/checks/<dataset>.yml` | **new** (1 per dataset) | `[PIPELINE]` |
| 5 | `tests/layer1_shaping/test_<entity>_shaping.py` | **new** | `[TESTS]` |
| 6 | `tests/layer2_contract/test_<entity>_contract.py` | **new** (3 tests) | `[TESTS]` |
| 7 | `tests/layer3_rules/test_<entity>_rules.py` | **new** (3 tests) | `[TESTS]` |
| 8 | `src/whoz_ingestion_etl/transformations/bronze/<entity>.py` | **new** | `[PIPELINE]` |
| 9 | `src/whoz_ingestion_etl/transformations/silver/<entity>.py` | **new** | `[PIPELINE]` |
| 10 | `resources/whoz_ingestion_etl.pipeline.yml` | changed | `[PIPELINE]` |
| 11 | *no file* — deploy and verify, in the order §11 gives | — | `[PIPELINE]` |
| 12 | `README.md`, `src/whoz_ingestion_etl/README.md` | changed | docs |

Eight or more new source files — five that ship to Databricks (steps 3, 4, 8, 9) and three
tests (steps 5, 6, 7) — plus the three fixtures and the data model doc. Only **one** existing
*shared code* file changes: `tests/conftest.py`. The pipeline config and the two READMEs
(steps 10 and 12 above) change too, but nothing else does — there is no shared test module to
edit, no shared rules file to append to, and no registry to update.

**All three test steps are new files, and that is the recent improvement.** Layers 2 and 3
used to be "append your three tests to the shared `test_schema_contract.py` /
`test_expectations.py`". They are now one file per entity per layer, which buys two things.
There is no shared test file for two people adding two entities to conflict on. And there is
nothing to *forget* appending to: a missing `tests/layer2_contract/test_<entity>_contract.py`
is a visibly absent file next to `test_talent_contract.py`, whereas a missing block inside a
600-line shared module is invisible and the suite stays green without it. Practically, steps
5, 6 and 7 are "copy the three talent files, s/talent/<entity>/, fix the expected values".

**Step 4 is pipeline code, not test code**, even though most of what the step says is about
tests. `whoz_ingestion/checks/` is loaded by the transformations — the checks genuinely run in
Databricks, evaluated by DQX. Both sides read them through the same `CHECKS` dict: the
pipeline passes `CHECKS["<dataset>"]` to `apply_checks_by_metadata`, and
`tests/layer3_rules/` passes the identical list to the identical call. That dual readership is
the entire reason the checks are data in a file, and it is what makes a test able to prove a
check resolves against real columns.

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
- [ ] **Primary key** — which field is stable and non-null? It becomes the dataset's one
      `drop` rule, and `NOT NULL` in its schema file.
- [ ] **Foreign keys** — which fields join to already-modelled tables (e.g. `profile_id` →
      `silver.whoz_profiles`)?
- [ ] **Sequencing field** — the source's own last-modified timestamp. AUTO CDC sequences
      by this, *not* by ingest time, so a late-landing backfill doesn't overwrite newer data.
      If the export has no such field, stop and decide what to do — SCD is not safe without one.
- [ ] **Polymorphic fields** — anything that arrives as int on some records and float on
      others, or object on some and array on others. Each one is a `hazards.json` record and
      probably a `warn` rule.
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
- [ ] **Every quality check must pass on this file.**

### 1b. `hazards.json` — awkward but legitimate

One record per documented type hazard from step 0. Awkward is not invalid — **every check
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

- [ ] **One record per quality check**, so expected counts are all `1` and a failure names
      exactly one check. Deviate only deliberately: the talent fixture's
      `violation-profile-as-array` trips two checks, and the test comment explains why that
      pair firing together is the point.
- [ ] Descriptive ids again (`violation-completion-rate-as-percentage`), except for the
      record violating the null-key check — that one's id *is* `null`.
- [ ] Each record should violate its check **and nothing else**, because
      `assert_dqx_violations` treats the expected-counts dict as complete: a record that also
      trips an unrelated check fails the test.

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

## 3. `[PIPELINE]` `schemas/<entity>.yml` + `shaping/<entity>.py` — the actual work

The seam that makes everything testable. Model it on `whoz_ingestion/shaping/talent.py` and
`whoz_ingestion/schemas/whoz_talent.yml`.

- [ ] **No `pyspark.pipelines` import. Ever.** That module only fully exists inside a running
      Lakeflow pipeline. Plain `DataFrame` in, plain `DataFrame` out is what lets a local
      SparkSession test it.
- [ ] **Header comment** stating what the module models and — explicitly — what it
      deliberately does not, and why.
- [ ] **`schemas/<entity>.yml`** — one `- name: / type: / comment:` entry per column, in
      order. `contract.ddl()` renders it into the string `create_streaming_table(schema=...)`
      takes; you never write DDL by hand and never escape an apostrophe.
  - [ ] Column order must match `shape_<entity>()`'s `select()` **exactly**. Order is part of
        the contract: AUTO CDC matches source to target positionally as well as by name.
  - [ ] `NOT NULL` in the `type:` only on the key the `drop` rule guards.
  - [ ] Omit `comment:` entirely for a column that has nothing to say; do not write an empty
        one. Unknown keys raise at load, so a typo'd `commnet:` fails rather than vanishing.
  - [ ] Comments should carry the analysis: units, observed distributions, which rule
        enforces the column, which table a foreign key points at.
- [ ] **`<ENTITY>_COLUMNS = ddl(load_columns("<entity>"))`** in the shaping module, and
      **`<ENTITY>_HISTORY_COLUMNS = ddl(<ENTITY>_COLUMN_DEFS + SCD2_COLUMNS)`** if the entity
      gets an SCD2 table — built there rather than at the call site, so the tests check the
      real string the pipeline uses. `SCD2_COLUMNS` is shared and already `TIMESTAMP`-typed to
      match `sequence_by`; do not re-declare the window columns per entity.
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

## 4. `[PIPELINE]` `whoz_ingestion/checks/<dataset>.yml` — the quality checks

One **new file per dataset**, named for the dataset, holding a flat
[DQX](https://databrickslabs.github.io/dqx/) check list — the exact format
`DQEngine.validate_checks` and `apply_checks_by_metadata` accept. Model it on
`checks/whoz_talent_shaped.yml`.

- [ ] **The filename is the dataset key** — identical to the `@dp.temporary_view` /
      `@dp.table` function name in `transformations/` the checks protect. Not a convention:
      `test_every_rule_set_is_applied_by_a_transformation` checks it both ways, so a typo'd
      key and a dataset nobody applies both fail locally.
- [ ] **An explicit `name:` on every check.** DQX generates one otherwise, and the generated
      name is not the one your expected-count dicts and the metric labels use. `checks.py`
      raises at import if a check has none.
- [ ] **`criticality: error`** → the row is **withheld** from the table and written only to
      quarantine. In practice this is exactly one check per dataset: the null primary key.
      Anything else is rejected by `test_error_criticality_checks_only_ever_guard_a_key`,
      which requires the check *function* to be `is_not_null`.
- [ ] **`criticality: warn`** → the row is in **both** the table and quarantine. Everything
      else lives here: source assumptions that should raise an eyebrow, not remove data, if
      they stop holding. Never omit `criticality:` — DQX defaults to `error`.
- [ ] **Prefer a built-in check function to `sql_expression`.** `is_not_null` for null checks,
      `is_in_range` for ranges; the full list is `databricks.labs.dqx.check_funcs`. Built-ins
      carry typed messages and tested edge handling, and `is_in_range` is null-safe by
      construction — so do *not* write the old `"x IS NULL OR …"` wrapper around one.
- [ ] **When you do need `sql_expression`, mind the null semantics and give it a `msg:`.**
      DQX flags a row when `NOT(<expression>)` is TRUE, so an expression that evaluates to
      NULL flags **nothing** — the opposite of the old `@dp.expect_all`, which flagged
      anything that was not TRUE. A two-column comparison where either side can be NULL needs
      an explicit guard (`profile_talent_id IS NULL OR (talent_id IS NOT NULL AND …)`) or it
      silently stops reporting. `test_check_is_well_formed` requires the `msg:`; only you can
      get the nulls right.
- [ ] **Check names must be unique across the whole project** — enforced by `checks.py` at
      import *and* by `test_loader_rejects_a_name_reused_across_two_datasets`, because names surface
      as metric labels where two identical names on different tables read as one. This is why
      the talent key check is `talent_pk_not_null`, not `talent_id_not_null`.
- [ ] **snake_case names**, enforced by `test_check_is_well_formed`.
- [ ] **Nothing to register.** There is no second list: `test_rule_hygiene.py` iterates
      `CHECKS` itself, so your checks are covered the moment the file exists.
- [ ] Every dataset in `checks/` must be **applied** in `transformations/` (step 9), as
      `dq.apply_checks_by_metadata(<query>, CHECKS["<dataset>"])`, and must have a `get_valid`
      consumer and a `get_invalid` quarantine table. Enforced by
      `test_every_rule_set_is_applied_by_a_transformation` and
      `test_every_checked_dataset_has_a_valid_view_and_a_quarantine_table`.
- [ ] Comment each check with *what it would mean if it fired*. That sentence is the entire
      value of the check at 3am — and when a built-in changes the predicate's meaning at all
      (null handling, most often), say so on the check.

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
      Proves the rendered DDL parses. Cheaper to keep than to reason about now that
      `contract.ddl()` does the quoting: it also covers a bad `type:` in the schema file,
      which nothing else in the toolchain notices.
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

*Are the quality checks themselves right?* This layer is two halves, and knowing which half
you are in is the whole point of the folder having three files in it:

- **`test_rule_hygiene.py` — you never touch it.** It iterates `CHECKS` and asserts, over
  *every* check in the project, that DQX validates it, that the name is snake_case and unique
  project-wide, that an `error` check only ever guards a key, that a `sql_expression` carries
  a `msg:`, and that the dataset keys in `checks/` and the datasets `transformations/` applies
  describe the same set — plus that each has a `get_valid` consumer and a quarantine table.
  Your checks start being covered by all of that the moment step 4's file exists. There is
  nothing to add here, and adding something here is a sign you have written a behavioural test
  in the wrong file.
- **`test_<entity>_rules.py` — a new file, three tests, and the half only you can write.**
  Hygiene proves a check is well-formed; it cannot know what the check is *supposed to mean*,
  and — the thing DQX makes urgent — it cannot know whether the check's columns exist.
  Copy `test_talent_rules.py`.

The three tests, in the new file:

- [ ] `<ENTITY>_CHECKS = CHECKS["<dataset>"]` at the top of the module, and a small `checked`
      fixture that returns `dq_engine.apply_checks_by_metadata(shape_<entity>(...),
      <ENTITY>_CHECKS)` — the same call the transformation makes, on the same list.
- [ ] `test_<entity>_rules_pass_on_valid_records` — parametrized over `["typical", "hazards"]`,
      `assert_no_dqx_violations`. A check firing here is a false positive, and a false positive
      trains everyone to ignore the quarantine table.
- [ ] `test_<entity>_rules_catch_the_records_designed_to_break_them` — `assert_dqx_violations`
      with the complete expected-count dict. Complete, not a subset: any check you don't list
      must have zero failures, which is what makes this catch an *over*-broad check and not
      just an under-broad one.
- [ ] `test_every_<entity>_rule_is_covered_by_the_violations_fixture` — the guard on the test
      above, failing until every check fires on something.
- [ ] **`assert_no_skipped_checks` in every one of them.** `assert_no_dqx_violations` and
      `assert_dqx_violations` call it for you; anything that only counts rows must call it
      itself. DQX does not raise on a column it cannot resolve — it marks the check
      `skipped=true` and carries on, which either quarantines every row or checks nothing.
      This assertion is the only thing in the project that sees that.
- [ ] A module docstring opening `"""Layer 3 — ... (<entity> behaviour)"""` that names
      `test_rule_hygiene.py` as the other half, so the next reader lands in the right file.
- [ ] Import `CHECKS` and `shape_<entity>`; keep the rationale in the docstring to a
      pointer at README.md#data-quality-checks rather than a fourth copy of it.

**Child datasets go in `test_child_rules.py` instead**, not a file of their own. Put the query
in its own `whoz_ingestion/shaping/<entity>_<collection>.py` as a function taking the source
relation, import it in `test_child_rules.py` and add it to that file's dataset map, and the
`child_query` fixture runs it over a bronze fixture so its checks resolve against real
columns. Skipping this is what used to leave half the project's checks parsed but never
resolved — and under DQX it would leave them *skipped*, which looks identical to a clean pass.

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

The wiring that turns step 3's shaping function and step 4's checks into real tables. It
should contain no logic of its own — if you find yourself writing a transformation here,
it belongs in `whoz_ingestion/shaping/` where it can be tested. Untested locally for the same
reason as step 8, and now more so: this file constructs a `DQEngine(WorkspaceClient())` at
import, so it needs a workspace as well as a pipeline.

- [ ] Import from `whoz_ingestion.shaping.<entity>` and `whoz_ingestion.checks`. Note the
      import root is `whoz_ingestion.x`, **not** `src.whoz_ingestion.x` or
      `whoz_ingestion_etl.whoz_ingestion.x` — the pipeline's `root_path` *is* `src`, and
      `pyproject.toml`'s `pythonpath` points pytest at the same folder, so a wrong prefix
      fails locally instead of at deploy.
- [ ] **One `dq = DQEngine(WorkspaceClient(), spark=spark)` at module scope.** No arguments to
      `WorkspaceClient()`: the SDK's default authentication resolves to the pipeline's own
      run-as identity inside a workspace. Pass `spark=spark` explicitly — Lakeflow injects it
      into this module's namespace, and using the injected session is the difference between
      correct and correct by coincidence.
- [ ] **No `ExtraParams`, unless you build a materialized view over DQX output.**
      `run_time` / `run_id` are non-deterministic per run, which breaks an MV's incremental
      refresh; `run_time_overwrite` / `run_id_overwrite` pin them. Nothing in this pipeline is
      such an MV today — the `_payload_shapes` monitors are MVs but carry no checks — so the
      parameter is deliberately absent. Say so in a comment rather than leaving it unremarked.
- [ ] **Three objects per checked dataset**, the documented DQX pattern:
      - [ ] `@dp.temporary_view` `<name>_checked` returning
            `dq.apply_checks_by_metadata(<the query>, CHECKS["<dataset>"])`. Applying the
            checks once, on the shaped view, means they run once and protect every downstream
            target.
      - [ ] The real dataset, returning
            `dq.get_valid(spark.readStream.table("<name>_checked"))`. **Keep its existing
            name** — AUTO CDC's `source=` names it, and `get_valid` drops `_errors`/`_warnings`
            so the schema is unchanged.
      - [ ] `@dp.table` `<name>_quarantine` with
            `table_properties={"quality": "quarantine"}`, returning
            `dq.get_invalid(spark.readStream.table("<name>_checked"))`. **No `schema=`** — let
            it infer, since it is the base columns plus DQX's two result arrays, whose nested
            struct DQX owns and may extend between minor releases.
      - [ ] A dataset with no checks gets none of this: one `@dp.table`, as before.
- [ ] `dp.create_streaming_table(schema=<ENTITY>_COLUMNS, table_properties={"quality":
      "silver"}, cluster_by=[...])` for the SCD1 table.
- [ ] `dp.create_auto_cdc_flow(target=..., source="<entity>_shaped", keys=[<pk>],
      sequence_by=F.col("source_last_modified_at"), stored_as_scd_type="1")`.
- [ ] The SCD2 pair, identical but `schema=<ENTITY>_HISTORY_COLUMNS` and
      `stored_as_scd_type="2"`.
- [ ] Child tables (one per nested array you chose to explode), **each in its own file under
      `transformations/silver/`, named for the table it produces** — never a second dataset
      appended to an existing module. Each is the same three objects, with
      `spark.sql(<name>_sql(f"STREAM({BRONZE_TABLE})"))` as the query the `_checked` view
      applies checks to. The query itself — `LATERAL variant_explode(b.payload:<field>)` over
      the source relation — belongs in its own `whoz_ingestion/shaping/<entity>_<collection>.py`,
      **not** inline here, or its checks cannot be resolved by any test and DQX will silently
      skip them. Each gets its own `checks/<name>.yml` named for the dataset function.
- [ ] Use the shared engine: `from whoz_ingestion.dq import engine` then `dq = engine(spark)`.
      Do **not** construct a `DQEngine` per module — each construction is two blocking
      workspace calls at graph init, and one file per table would otherwise multiply them.
      A module with no checks (like `whoz_profile_skill_ratings.py`) imports neither.
- [ ] For any parsed date/number in a child table, keep **both** the parsed value and the raw
      string (`since_date` + `since_raw`), so a value the cast can't handle is visible rather
      than just NULL — and add the matching `*_parsed` warn check.

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
      green on a broken DDL string or a check DQX will skip. That's what layers 2 and 3
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
- [ ] **Confirm the `_checked` views attached and the quarantine tables exist.** Nothing local
      can check that `spark.readStream.table("<name>_checked")` resolves a temporary view in
      the pipeline graph, that `WorkspaceClient()` authenticates on serverless, or that a
      quarantine table infers a usable schema from DQX's result structs. The first update is
      the only test of any of it.
- [ ] **Query the quarantine tables after that first run — and look for `skipped` first.**
      The pipeline's Data Quality tab does *not* populate for DQX-checked datasets (DQX does
      not use Expectations), so quarantine is where findings live now.

      ```sql
      SELECT r.name, r.message, r.skipped, count(*) AS rows
      FROM silver.<table>_quarantine
      LATERAL VIEW explode(concat(coalesce(_errors, array()), coalesce(_warnings, array()))) AS r
      GROUP BY r.name, r.message, r.skipped ORDER BY rows DESC;
      ```

      Any row with `skipped = true` means DQX could not resolve that check's columns and
      enforced nothing (or quarantined everything). A `warn` check firing broadly is
      information: either the source is different than you analysed, or the check is wrong.
      Both are worth knowing before the promotion to `dev`.
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
| Applying a dataset's checks in `transformations/` | `test_every_rule_set_is_applied_by_a_transformation`, locally — and the same test catches a typo'd `CHECKS["..."]` key, which would otherwise be a `KeyError` at pipeline update |
| The `get_valid` consumer or the `_quarantine` table | `test_every_checked_dataset_has_a_valid_view_and_a_quarantine_table`, locally |
| A `violations.json` record for a new check | `test_every_<entity>_rule_is_covered_by_the_violations_fixture`, locally |
| Writing steps 6 and 7's files at all | Nothing, ever. Hygiene still passes on your checks; a per-entity test file that does not exist fails nothing. One-file-per-entity-per-layer makes that gap **visible** — an empty slot next to `test_talent_contract.py` — it does not make it enforced. This is the row the folder layout improves and does not close. |
| Keeping the DDL in step with the `select()` | `test_declared_schema_matches_shape_<entity>_output`, locally |
| A bad `type:` in `schemas/<entity>.yml` | `test_<entity>_columns_is_valid_ddl`, locally. Apostrophes are no longer your problem — `contract.ddl()` escapes them |
| A typo'd key in a column entry (`commnet:`) | `contract.load_columns`, at import — in pytest and in the pipeline alike |
| A non-key check at `criticality: error` | `test_error_criticality_checks_only_ever_guard_a_key`, locally |
| A misspelled check function, a bad argument name, an invalid criticality | `whoz_ingestion.checks`, at import, via `DQEngine.validate_checks`. Deliberately in the loader, not only in a test: silently enforcing nothing is the failure this prevents |
| A duplicate check name | `whoz_ingestion.checks` at import, and `test_loader_rejects_a_name_reused_across_two_datasets` locally |
| A typo'd **column** in a check | `test_<entity>_rules.py` or `test_child_rules.py`, locally, via `assert_no_skipped_checks`, for **every** dataset — provided its query lives in `whoz_ingestion/shaping/`. `validate_checks` cannot see this; only applying the checks to real columns can. Write a query inline in `transformations/silver/` instead and you re-open the gap: DQX marks the check `skipped=true` at pipeline update, and *nothing fails* — the table either empties into quarantine or reports a clean pass forever. |
| `to_utc_strings()` on a timestamp assertion | Nothing locally; CI, on a UTC runner |
| Matching `BRONZE_KEYS` to the bronze `try_variant_get` paths | Nothing, anywhere. The tests keep passing and production is wrong. Check this one by eye. |
| A correct `FILE_NAME_GLOB` | Nothing. Auto Loader finds no files and reports a healthy run. |
| The three-object wiring being *correct* (not just present) | Nothing locally. `bundle validate` does not resolve `spark.readStream.table("<name>_checked")`, does not authenticate `WorkspaceClient()`, and does not infer the quarantine schema. Step 11's first deploy is the only test. |

The last three are the framework's real blind spots, and it is no coincidence that they belong
to **steps 8 and 9**: the `[TESTS]` track can only reach as far as the `whoz_ingestion/` seam,
so the one place a mistake survives to production is the `[PIPELINE]`-only files it can't
execute. Everything above those rows is caught before merge — except the "steps 6 and 7" row,
which is caught by review rather than by pytest. Those get read by a human or they get read
by nobody.

---

## If it's a whole new source system, not just an entity

Everything above, plus:

- [ ] `src/<source>_etl/` alongside `whoz_ingestion_etl/`, holding only `transformations/`
      (same `bronze/` + `silver/` layer-first split), **plus** a sibling `src/<source>/`
      shared package alongside `whoz_ingestion/` (`shaping/` + `schemas/` + `checks/` and
      their loaders).
      One pair per source, never one shared package for two sources: a rename or a check
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

# whoz_ingestion_etl

The Lakeflow pipeline for the Whoz source. This folder holds **only** what Lakeflow
loads and executes:

- `transformations/`: all dataset definitions, split layer-first into `bronze/` and
  `silver/`. Layer-first rather than entity-first because a future `gold/` would join
  across entities and needs a peer folder to live in.

Everything these files *call* lives one level up, in `../whoz_ingestion/` — the shared
package, which imports no `pyspark.pipelines` and is therefore unit-testable:
`shaping/<entity>.py` (DataFrame-in/DataFrame-out row logic), `shaping/<table>.py` (one
module per exploded child table, named for the entity that owns it, each returning SQL text
that takes the source relation as an argument), `schemas/<entity>.yml` + `contract.py` (each
table's columns as data, rendered into the DDL string `create_streaming_table` takes),
`checks/<dataset>.yml` + `checks.py` (every data quality check, as a native Databricks Labs
DQX check list, one file per dataset) and `dq.py` (the pipeline's one DQEngine, built lazily
so importing the package never needs credentials). The tests import those same modules. The
import prefix is `whoz_ingestion.x` in both places, because the pipeline's `root_path` is
`src/` rather than this folder — see `../whoz_ingestion/__init__.py`.

**One file per silver table.** Every module under `transformations/silver/` is named for the
table it produces, so "where is this table defined" needs no search. A checked dataset's three
objects all live in its own file; the `_checked` view is never shared across files.

## Data quality: the three-object shape

Quality is enforced by DQX at runtime, not by `@dp.expect_all` — there are no expectation
decorators in this folder. Every dataset that has checks is **three** objects: a
`<name>_checked` view that applies them, the real table fed by `get_valid`, and a
`<name>_quarantine` table fed by `get_invalid`.

**The shape, what `get_valid` returns, and what `error` vs `warn` costs are documented once,
in [the root README](../../README.md#how-a-checked-dataset-is-wired).** Read it there; this
section deliberately does not restate it, because the two copies drifted apart the first time
and a wrong description of the criticality contract is worse than no description.

Only the folder-specific part lives here: `whoz_profile_skill_ratings` and
`whoz_profile_completion_rules` have no checks, so they are a single `@dp.table` each, with
no check view and no quarantine table.

## Getting Started

Most of the relevant source code lives under `transformations/`, one entity at a time.

### Profile — the Whoz profile export

* `bronze/whoz_profiles.py` — Auto Loader ingestion into `bronze.whoz_profiles` (whole
  JSON object kept in one VARIANT column), plus the
  `bronze.whoz_profiles_payload_shapes` drift monitor. No checks on either.
* `silver/whoz_profile.py` — `silver.whoz_profiles` (current state, AUTO CDC SCD1),
  `silver.whoz_profile_history` (full version history, AUTO CDC SCD2) and
  `silver.whoz_profiles_quarantine`. The profile grain only.
* `silver/whoz_profile_aptitudes.py` — `silver.whoz_profile_aptitudes` + its `_checked` view
  and `_quarantine` table.
* `silver/whoz_profile_positions.py` — `silver.whoz_profile_positions`, same three objects.
* `silver/whoz_position_aptitude_refs.py` — `silver.whoz_position_aptitude_refs`, same three.
* `silver/whoz_profile_completion_rules.py` — the completionDetails map, exploded. No checks,
  so one `@dp.table` and nothing else.
* `silver/whoz_profile_skill_ratings.py` — the legacy array. No checks, so one `@dp.table`.

### Talent — the Whoz talent export

A separate export with its own file, its own Auto Loader stream and its own schema
location; it is not a variant of the profile one.

* `bronze/whoz_talents.py` — ingestion into `bronze.whoz_talents` (payload includes the
  nested `profile` object, untouched), plus the `bronze.whoz_talents_payload_shapes`
  drift monitor, which groups by top-level key set *and* by the container type of that
  nested profile.
* `silver/whoz_talent.py` — `silver.whoz_talents` (current state, AUTO CDC SCD1),
  `silver.whoz_talent_versions` (full version history, AUTO CDC SCD2) and
  `silver.whoz_talents_quarantine`. The talent grain only. The embedded `profile` object is
  deliberately not re-modelled here — `silver.whoz_profiles` already does that, and this
  table carries `profile_id` as a foreign key into it.
* `silver/whoz_talent_workspace_history.py` — the source's own `history[]` array, exploded,
  with its `_checked` view and `_quarantine` table. The only child dataset in the pipeline
  that reads `bronze.whoz_talents` rather than `bronze.whoz_profiles`.

  The two "history" names are not interchangeable, which is why they are now two files:
  `whoz_talent_versions` is how the talent *record* changed over time (derived by AUTO CDC in
  the file above); `whoz_talent_workspace_history` is which workspaces the talent has belonged
  to (the source's own array).

### User — the Whoz user export

The **login account**, not the person. `silver.whoz_talents` is a person in a workspace; the
two are not 1:1 in either direction (1,760 accounts have no talent, 1,841 talents have no
account), so a headcount built on `silver.whoz_users` is counting accounts.

* `bronze/whoz_users.py` — ingestion into `bronze.whoz_users`, plus the
  `bronze.whoz_users_payload_shapes` drift monitor, which groups by top-level key set **and**
  by the container type of `workspaceRoles` and `federationRoles` — both arrive as an OBJECT
  when populated and an empty ARRAY when not.
* `silver/whoz_user.py` — `silver.whoz_users` (current state, AUTO CDC SCD1),
  `silver.whoz_user_versions` (full version history, AUTO CDC SCD2) and
  `silver.whoz_users_quarantine`. The account grain only. Federation membership is flattened
  onto the row here because it is strictly one entry per record today;
  `federation_count_at_most_one` is the check that fires when that stops being true.
* `silver/whoz_user_workspace_roles.py` — `silver.whoz_user_workspace_roles`, one row per
  (user, workspace, role), with its `_checked` view and `_quarantine` table.

Bronze and silver are their own schemas, literally named `bronze` and `silver`, in
**every** target — including your personal sandbox. What changes between environments is
the catalog, not the schema: the catalog is the isolation boundary. See the
`CATALOG`/`BRONZE_SCHEMA`/`SILVER_SCHEMA` constants at the top of each file, and
`bronze_schema`/`silver_schema` in `../../databricks.yml`, which are `bronze`/`silver` in
all four targets.

The source data model, the field inventory and the type hazards these choices work
around are documented in `../../docs/whoz_profile_data_model.md`. Read it before
changing any of the casts.

* By convention, every dataset under `transformations` is in a separate file.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* If you're using the workspace UI, use `Run file` to run and preview a single transformation.
* If you're using the CLI, use `databricks bundle run whoz_ingestion_etl --select whoz_profiles` to run a single transformation.

For more tutorials and reference material, see https://docs.databricks.com/dlt.

# =====================================================================================
# SILVER — silver.whoz_profiles / silver.whoz_profile_history
#
# Profile-level (1 row per profile) flattening of the bronze VARIANT payload. This file
# holds ONLY the profile grain; each collection exploded out of the payload is its own
# file in this folder, named for the table it produces:
#
#   whoz_profile_aptitudes.py        whoz_profile_positions.py
#   whoz_position_aptitude_refs.py   whoz_profile_skill_ratings.py
#   whoz_profile_completion_rules.py
#
# Every extraction uses try_variant_get so that a bad value nulls one column instead
# of failing the update. Collections stay as VARIANT here — they are unnested in the
# child tables.
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# The shared code lives in the sibling src/whoz_ingestion/ package, not under this
# pipeline folder: the pipeline's root_path IS src/ (see
# resources/whoz_ingestion_etl.pipeline.yml), so src/ itself is on sys.path at runtime and
# `whoz_ingestion.x` resolves. tests/ import by this same path (see pyproject.toml's
# pythonpath) so a wrong prefix here fails the test suite too, not only at deploy time.
from whoz_ingestion.checks import CHECKS
from whoz_ingestion.dq import engine
from whoz_ingestion.shaping.profile import PROFILE_COLUMNS, PROFILE_HISTORY_COLUMNS, shape_profile

CATALOG = spark.conf.get("whoz.catalog")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles"

# ONE engine for the whole pipeline, not one per module, and lazily built. Constructing a
# DQEngine is a live workspace call that can fail the update — it verifies connectivity twice
# per construction, unconditionally — so a shared instance is what keeps this silver layer at
# 2 control-plane round trips rather than one pair per file. The full reasoning, including why
# it must stay lazy for pytest to collect at all and why no ExtraParams pinning is needed,
# lives once in whoz_ingestion/dq.py.
dq = engine(spark)


# -------------------------------------------------------------------------------------
# Checked rows off bronze — shaped, then annotated by DQX with _errors and _warnings.
# Pipeline-scoped; it exists so the check list is written down in one place and both the
# valid view and the quarantine table below are defined against the same expression.
#
# IT DOES NOT MEAN THE CHECKS RUN ONCE, which is what this comment used to say. A pipeline
# view is not materialized: Databricks recomputes it per consumer. This one has three —
# the quarantine table, and both AUTO CDC flows via whoz_profile_shaped — so bronze is
# streamed three times and shape_profile() plus all four checks are evaluated three times,
# each flow carrying its own checkpoint.
#
# The consequence worth knowing is not the cost, it is that silver and quarantine advance
# on SEPARATE checkpoints. "A warn row is in both" is therefore eventually true, not
# atomically true, and a full refresh of one and not the other desynchronizes them until
# both are refreshed together.
#
# The check list is data in whoz_ingestion/checks/whoz_profile_shaped.yml — a native DQX
# check list, keyed here by that file's name. tests/layer3_rules/test_profile_rules.py
# applies the very same list to real shape_profile() output without a pipeline, which is
# what makes a check naming a column that does not exist fail in pytest rather than being
# silently skipped here (DQX marks an unresolvable check `skipped=true` and carries on —
# see whoz_ingestion/checks.py and helpers.assert_no_skipped_checks).
# test_rule_hygiene.py checks the key below both ways: that it exists in CHECKS, and that
# every dataset in CHECKS is one an apply_checks_by_metadata call here actually applies.
# -------------------------------------------------------------------------------------
@dp.temporary_view
def whoz_profile_checked():
    return dq.apply_checks_by_metadata(
        shape_profile(spark.readStream.table(BRONZE_TABLE)), CHECKS["whoz_profile_shaped"]
    )


# -------------------------------------------------------------------------------------
# The rows that pass — pipeline-scoped, materializes nothing itself.
# Whoz re-lands a full snapshot under each dated export (see bronze/whoz_profiles.py),
# so the same profile_id shows up once per snapshot here. Both AUTO CDC flows below
# read this same view and turn that into an upsert, keyed by profile_id. A temporary
# view is never a catalog object, so it keeps its bare name — nothing to qualify, and an
# unqualified name is how Lakeflow resolves one pipeline dataset from another.
#
# The name is unchanged from before DQX, deliberately: create_auto_cdc_flow(source=...)
# below still names this view, and get_valid() returns the base columns WITHOUT _errors
# and _warnings, so the schema both flows merge into is byte-for-byte what it was.
# get_valid excludes rows that failed an `error` check and keeps rows that only failed a
# `warn` one — which is exactly the old expect_all_or_drop / expect_all split.
# -------------------------------------------------------------------------------------
@dp.temporary_view
def whoz_profile_shaped():
    return dq.get_valid(spark.readStream.table("whoz_profile_checked"))


# -------------------------------------------------------------------------------------
# The rows that did not — every row anything fired on, with the struct saying what.
#
# NOT a dead-letter queue: `warn` rows are in here AND in silver.whoz_profiles, because a
# warn-level finding is an eyebrow raised, not data withheld. Filter on
# `_errors IS NOT NULL` for the rows that were actually kept out of silver.
#
# No explicit schema=. Everywhere else in this pipeline the schema is declared (see
# PROFILE_COLUMNS) because AUTO CDC needs it and because a hand-written DDL string fails
# silently — but this table is the base columns plus DQX's two result arrays, whose struct
# DQX owns and may extend between minor releases. Declaring it here would mean re-deriving
# an 11-field nested struct by hand and re-deriving it again on every DQX upgrade, to buy
# nothing: no AUTO CDC flow reads this table, so nothing depends on its column order.
# -------------------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profiles_quarantine",
    comment=(
        "Profile rows that failed a DQX check, with _errors/_warnings naming which. Rows with "
        "_errors were kept OUT of silver.whoz_profiles; rows with only _warnings are in both."
    ),
    table_properties={"quality": "quarantine"},
)
def whoz_profiles_quarantine():
    return dq.get_invalid(spark.readStream.table("whoz_profile_checked"))


# whoz_profiles — SCD Type 1: one row per profile_id, current state only. Each new
# snapshot upserts in place; the previous version of a changed profile is gone.
dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profiles",
    comment=(
        "One row per Whoz profile, current state only — see whoz_profile_history for prior "
        "versions. No 'payload' column: query bronze.whoz_profiles by profile_id for the raw JSON."
    ),
    schema=PROFILE_COLUMNS,
    table_properties={"quality": "silver"},
    cluster_by=["federation_id", "profile_id"],
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profiles",
    source="whoz_profile_shaped",
    keys=["profile_id"],
    # Whoz's own last-modified time on the record, not when we happened to ingest it —
    # protects against an older dated export landing after a newer one (e.g. backfill).
    sequence_by=F.col("source_last_modified_at"),
    stored_as_scd_type="1",
    # No except_column_list: the source view no longer carries a payload column at all
    # (see shape_profile). It used to, and had to be excluded here, because AUTO CDC
    # compares whole rows with `<=>` to detect real changes and VARIANT does not support
    # that comparison — it failed with INVALID_ORDERING_TYPE. Not selecting the column
    # in the first place solves the same problem and saves reading it.
)

# whoz_profile_history — SCD Type 2: every version of every profile, each row valid
# for __START_AT to __END_AT (NULL __END_AT = still current). Same keys/ordering as
# whoz_profiles above; only the storage type differs.
dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_history",
    comment=(
        "Full version history of whoz_profiles, one row per (profile_id, version). "
        "__START_AT/__END_AT mark each version's validity window; NULL __END_AT is current. "
        "No 'payload' column: query bronze.whoz_profiles by profile_id for the raw JSON."
    ),
    # SCD2 requires __START_AT/__END_AT in an explicit schema, typed to match sequence_by
    # (source_last_modified_at, TIMESTAMP). Both the concatenation and that type rule live
    # in PROFILE_HISTORY_COLUMNS so tests/layer2_contract/test_profile_contract.py checks
    # this exact string.
    schema=PROFILE_HISTORY_COLUMNS,
    table_properties={"quality": "silver"},
    cluster_by=["profile_id"],
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_history",
    source="whoz_profile_shaped",
    keys=["profile_id"],
    sequence_by=F.col("source_last_modified_at"),
    stored_as_scd_type="2",
    # See the note on whoz_profiles' flow above. This SCD2 flow is the one that actually
    # hit the VARIANT comparison failure, since version-boundary detection diffs
    # consecutive rows for a profile_id; it is fixed at the source now, not excluded.
)


# -------------------------------------------------------------------------------------
# Not modelled on purpose — empty on every one of the 4,113 records in this extract:
#   customFields, functionalDomains, schedules, targetFunctionalDomains,
#   targetSkillRatings, positions[].customFields, headline.mobilityDestinations
#
# Rather than build empty tables, watch for them filling up. Add this check to
# whoz_ingestion/checks/whoz_profile_shaped.yml and it will alert the first time Whoz starts
# sending any of them — no change to this file, since the check view above already applies
# whatever that file holds:
#
#   - name: unmodelled_collections_still_empty
#     criticality: warn
#     check:
#       function: sql_expression
#       arguments:
#         expression: coalesce(custom_field_count, 0) = 0
#         msg: Whoz has started populating a collection this pipeline does not model
#
# (it would need the corresponding size_of() column added to shaping/profile.py first —
# the shaped view carries no payload column, deliberately.)
#
# Same idea for qualificationIds[] and targetSkills[] — 423 and 2 values respectively,
# too thin to be worth a table today.
#
# This note sits here, on the profile grain, because that is where such a check would go.
# The collections that ARE modelled each have their own file in this folder.
# -------------------------------------------------------------------------------------

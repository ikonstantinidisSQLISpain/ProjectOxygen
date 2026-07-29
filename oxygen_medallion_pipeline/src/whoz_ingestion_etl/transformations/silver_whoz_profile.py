# =====================================================================================
# SILVER — silver.whoz_profiles / silver.whoz_profile_history
#
# Profile-level (1 row per profile) flattening of the bronze VARIANT payload.
# Child collections are handled in silver_whoz_profile_children.py.
#
# Every extraction uses try_variant_get so that a bad value nulls one column instead
# of failing the update. Collections stay as VARIANT here — they are unnested in the
# child tables.
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Not "whoz_ingestion_etl.utilities...": the pipeline's root_path IS src/whoz_ingestion_etl,
# so that folder itself is on sys.path at runtime, not its parent. "whoz_ingestion_etl" is
# never a valid import prefix here.
from utilities.profile_shaping import shape_profile

# See bronze_whoz_profiles.py for why these are read from pipeline config instead of
# hardcoded — bronze/silver share a schema in dev, split in prod.
CATALOG = spark.conf.get("whoz.catalog", "main")
BRONZE_SCHEMA = spark.conf.get("whoz.bronze_schema", "bronze")
SILVER_SCHEMA = spark.conf.get("whoz.silver_schema", "silver")
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.whoz_profiles"


# -------------------------------------------------------------------------------------
# Shaped, validated rows off bronze — pipeline-scoped, materializes nothing itself.
# Whoz re-lands a full snapshot under each dated export (see bronze_whoz_profiles.py),
# so the same profile_id shows up once per snapshot here. Both AUTO CDC flows below
# read this same view and turn that into an upsert, keyed by profile_id. A temporary
# view is never a catalog object, so it keeps its bare name — nothing to qualify.
# -------------------------------------------------------------------------------------
@dp.temporary_view
@dp.expect_or_drop("profile_id_not_null", "profile_id IS NOT NULL")
@dp.expect("talent_id_not_null", "talent_id IS NOT NULL")
# The source is unversioned in this extract (main=true everywhere), but the model
# allows several versions per talent — this warns if that ever starts happening.
@dp.expect("is_main_version", "is_main_version = true")
def whoz_profile_shaped():
    return shape_profile(spark.readStream.table(BRONZE_TABLE))


# Explicit schema for both tables below: an enforced contract (a future edit to
# shape_profile() that silently changes a type fails loudly at deploy/run time instead
# of drifting quietly), and it puts column comments where Catalog Explorer and
# DESCRIBE TABLE EXTENDED actually show them, for anyone browsing the catalog who never
# opens this file. Order matches shape_profile()'s SELECT; excludes payload, which
# except_column_list drops from both AUTO CDC flows below (see the comment there for
# why — bronze keeps the full raw payload regardless).
#
# Distributions and caveats named here (e.g. "false on every record today") describe
# the current export, not a guarantee — see docs/whoz_profile_data_model.md for the
# full field inventory this was pulled from.
_PROFILE_COLUMNS = """
    profile_id STRING NOT NULL COMMENT 'Whoz profile ID, stable primary key. NOT NULL is enforced upstream by profile_id_not_null (expect_or_drop)',
    talent_id STRING COMMENT 'One profile per talent today; the model allows several versions per talent',
    federation_id STRING COMMENT 'Tenant identifier; a single value across this whole export (single-tenant)',
    version_name STRING COMMENT '"Main version" on every record today',
    is_main_version BOOLEAN COMMENT 'true on every record today; is_main_version expectation upstream warns if that ever changes',
    status STRING COMMENT 'DRAFT | VALIDATED | SUBMITTED',
    content_language STRING COMMENT 'en / fr / nl / it / de / es, per docs/whoz_profile_data_model.md',
    permission_scope STRING COMMENT 'SECRET on every record today',
    travel_range STRING COMMENT 'DEFAULT on every record today',
    is_removed BOOLEAN COMMENT 'false on every record today',
    resume_relation_status STRING COMMENT 'Includes a typo in the source enum: RESUME_IMPORT_SUGGESTION_SUBMITED',
    completion_rate DOUBLE COMMENT 'Profile completeness score, 0-100. Source has both int and float; always cast to double',
    completion_rate_computed_at TIMESTAMP COMMENT 'When completion_rate was last computed',
    headline_job_title STRING COMMENT 'From the embedded headline object, absent on roughly a quarter of profiles',
    seeking_opportunities BOOLEAN COMMENT 'Non-null on very few profiles today',
    seeking_opportunities_updated_at TIMESTAMP,
    headline_permission_scope STRING COMMENT 'SECRET on every record today',
    headline_aim STRING COMMENT 'Null on every record today; kept so the column exists once Whoz starts populating it',
    national_mobility STRING COMMENT 'Null on every record today; kept so the column exists once Whoz starts populating it',
    international_mobility STRING COMMENT 'Null on every record today; kept so the column exists once Whoz starts populating it',
    mobility_date_raw STRING COMMENT 'Null on every record today; kept so the column exists once Whoz starts populating it',
    mobility_note STRING COMMENT 'Null on every record today; kept so the column exists once Whoz starts populating it',
    hobbies STRING COMMENT 'Free text',
    source_created_at TIMESTAMP COMMENT 'When Whoz created this profile record',
    source_created_by STRING COMMENT 'Whoz user ObjectId',
    source_last_modified_at TIMESTAMP COMMENT 'Whoz''s own last-modified time on the record — what AUTO CDC sequences by below, not our ingest time',
    source_last_modified_by STRING,
    source_last_explicit_update_at TIMESTAMP,
    source_last_explicit_update_by STRING,
    aptitude_count INT COMMENT 'size(aptitudes[]) at the source; exploded rows live in silver.whoz_profile_aptitudes',
    position_count INT COMMENT 'size(positions[]) at the source; exploded rows live in silver.whoz_profile_positions',
    skill_rating_count INT COMMENT 'size(skillRatings[]) at the source; legacy, mostly zero, see silver.whoz_profile_skill_ratings',
    qualification_count INT COMMENT 'size(qualificationIds[]) at the source; too thin (423 values total) to model as its own table',
    source_file STRING COMMENT 'Bronze lineage: which landed file this profile version came from',
    ingested_at TIMESTAMP COMMENT 'Bronze lineage: when this snapshot was ingested, not when Whoz generated it'
"""


# whoz_profiles — SCD Type 1: one row per profile_id, current state only. Each new
# snapshot upserts in place; the previous version of a changed profile is gone.
dp.create_streaming_table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profiles",
    comment=(
        "One row per Whoz profile, current state only — see whoz_profile_history for prior "
        "versions. No 'payload' column: query bronze.whoz_profiles by profile_id for the raw JSON."
    ),
    schema=_PROFILE_COLUMNS,
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
    # AUTO CDC compares whole rows across versions to detect real changes, and VARIANT
    # doesn't support the `<=>` comparison that needs — confirmed the hard way, this
    # broke whoz_profile_history's SCD2 flow below with INVALID_ORDERING_TYPE the
    # moment there was prior state to compare against. Excluding payload here too even
    # though this SCD1 flow hasn't shown the same failure yet: it's the same
    # column-comparison mechanism, so a second run (once there's a prior row to diff
    # against) would very likely hit it too. Bronze keeps the full raw payload forever
    # regardless, so nothing is actually lost by not duplicating it here.
    except_column_list=["payload"],
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
    # SCD2 requires __START_AT/__END_AT in an explicit schema, typed to match
    # sequence_by (source_last_modified_at, TIMESTAMP) — confirmed against the docs
    # before writing this, not guessed; get it wrong and the flow below fails to attach.
    schema=_PROFILE_COLUMNS + """,
    __START_AT TIMESTAMP COMMENT 'Start of this version''s validity window (SCD2, added by AUTO CDC)',
    __END_AT TIMESTAMP COMMENT 'End of this version''s validity window; NULL means still current (SCD2, added by AUTO CDC)'
""",
    table_properties={"quality": "silver"},
    cluster_by=["profile_id"],
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_history",
    source="whoz_profile_shaped",
    keys=["profile_id"],
    sequence_by=F.col("source_last_modified_at"),
    stored_as_scd_type="2",
    # See the same option on whoz_profiles' flow above — this is the one that actually
    # failed: [DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] "The <=> does not support
    # ordering on type VARIANT", from AUTO CDC's SCD2 version-boundary detection trying
    # to compare payload across consecutive rows for this profile_id.
    except_column_list=["payload"],
)


# =====================================================================================
# SILVER — silver.whoz_profile_completion_rules
#
# completionDetails is a MAP keyed by rule name, NOT a struct — and it arrives as an
# empty ARRAY [] on 723 of the 4,113 records. Exploding it to one row per
# (profile, rule) means a new scoring rule from Whoz shows up as new *rows*, not as a
# schema change, and the array-vs-object polymorphism is handled by the filter below.
# =====================================================================================
@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA}.whoz_profile_completion_rules",
    comment="One row per (profile, completion rule) — unpacked from the completionDetails map.",
    table_properties={"quality": "silver"},
)
def whoz_profile_completion_rules():
    return spark.sql(f"""
        SELECT
            b.profile_id,
            b.talent_id,
            e.key                                                  AS rule_name,
            try_variant_get(e.value, '$.satisfied', 'boolean')      AS is_satisfied,
            try_variant_get(e.value, '$.weight',    'int')          AS weight,
            b.ingested_at
        FROM STREAM({BRONZE_TABLE}) AS b,
             LATERAL variant_explode(b.payload:completionDetails) AS e
        -- variant_explode on an object yields key/value; on the empty-array form it
        -- yields no rows at all, which is exactly what we want.
        WHERE e.key IS NOT NULL
    """)

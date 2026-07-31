"""Layer 1 — does the transformation code do what we intended?

Unit tests for shape_profile(), the bronze -> silver row shaping behind
silver.whoz_profiles and silver.whoz_profile_history.

Scope note: @dp.table-decorated functions can't be imported outside a running pipeline
(their expectation decorators don't exist in open-source pyspark.pipelines, and the
module-level spark.conf.get calls fail with no pipeline configuration). That is why the
shaping logic lives in utilities/profile_shaping.py with no pipelines dependency at all,
and silver_whoz_profile.py is a thin wrapper that imports and decorates it. Test the
utilities function; don't try to call the @dp.table one.

What belongs here: one test per *documented hazard* in docs/whoz_profile_data_model.md,
plus the plain happy path. Each test names the hazard it pins down, so when Whoz changes
something the failure says which assumption broke rather than just "shaping is wrong".
"""

from pyspark.sql import DataFrame

# Imported exactly the way the pipeline imports it — see pyproject.toml's pythonpath.
# Not "whoz_ingestion_etl.utilities...", which resolves for neither.
from utilities.profile_shaping import shape_profile


def by_profile_id(df: DataFrame) -> dict:
    """{profile_id: Row} — lets a test address one fixture record by name.

    Fixture records in hazards.json/violations.json use descriptive ids
    ("hazard-absent-headline") rather than realistic 24-hex ObjectIds precisely so these
    lookups read as the thing being tested. typical.json uses realistic ids, since being
    realistic is what that file is for.
    """
    return {row["profile_id"]: row for row in df.collect()}


# -------------------------------------------------------------------------------------
# Happy path
# -------------------------------------------------------------------------------------
def test_typical_records_shape_without_loss(profile_fixture):
    result = shape_profile(profile_fixture("typical"))
    rows = by_profile_id(result)

    assert len(rows) == 2
    row = rows["6a1f4c2e8b3d9f0a1c2e4b60"]
    assert row["talent_id"] == "5b2e3d4c1a0f9e8d7c6b5a41"
    assert row["status"] == "VALIDATED"
    assert row["content_language"] == "en"
    assert row["headline_job_title"] == "Data Engineer"
    assert row["completion_rate"] == 0.68
    assert row["is_main_version"] is True
    assert row["is_removed"] is False
    # Collection sizes are counted off the payload, not off the child tables.
    assert row["aptitude_count"] == 2
    assert row["position_count"] == 1
    assert row["skill_rating_count"] == 0
    assert row["qualification_count"] == 1
    # Lineage columns pass through from bronze untouched.
    assert row["source_file"].endswith("whoz__profile_report_anonymized.json")
    assert row["ingested_at"] is not None


def test_empty_input_produces_no_rows_not_an_error(profile_bronze):
    # A landing file that happens to contain no profiles must shape to zero rows rather
    # than raise — the pipeline should stay up over an empty export.
    result = shape_profile(profile_bronze([]))

    assert result.count() == 0


# -------------------------------------------------------------------------------------
# Type hazards — docs/whoz_profile_data_model.md, "Type hazards"
# -------------------------------------------------------------------------------------
def test_completion_rate_normalizes_int_and_float_to_double(profile_fixture):
    # completionRate is an int on 1,859 records and a float on 2,254. It is a 0-1
    # fraction, not a 0-100 percentage (verified against the live export), and the int
    # form only occurs at the ends of that range — exactly where the hazard shows up.
    result = shape_profile(profile_fixture("hazards"))
    rows = by_profile_id(result)

    assert rows["hazard-completion-rate-int"]["completion_rate"] == 1.0
    assert rows["hazard-completion-rate-float"]["completion_rate"] == 0.425
    assert dict(result.dtypes)["completion_rate"] == "double"


def test_absent_headline_vs_null_field_both_resolve_to_null(profile_fixture):
    # headline.aim is present but null on every real record; headline itself is entirely
    # absent on ~25% of profiles. Both must surface as NULL in silver — the VARIANT
    # absent-vs-null distinction is preserved on the raw payload in bronze, and
    # deliberately flattened away here.
    rows = by_profile_id(shape_profile(profile_fixture("hazards")))

    absent = rows["hazard-absent-headline"]
    assert absent["headline_aim"] is None
    assert absent["headline_job_title"] is None

    present_but_null = rows["hazard-null-headline-fields"]
    assert present_but_null["headline_aim"] is None
    assert present_but_null["headline_job_title"] == "Engineer"


def test_sparse_record_nulls_columns_instead_of_failing(profile_fixture):
    # try_variant_get on a genuinely sparse record must not raise. This is the property
    # that keeps one malformed record from killing a whole batch.
    row = by_profile_id(shape_profile(profile_fixture("hazards")))["hazard-sparse-record"]

    assert row["talent_id"] == "t-sparse-record"
    assert row["status"] is None
    assert row["completion_rate"] is None
    assert row["source_last_modified_at"] is None
    # size() of an absent collection is NULL, not 0 — worth pinning, since a downstream
    # SUM over these treats the two differently without saying so.
    assert row["aptitude_count"] is None


def test_timestamps_parse_at_every_precision_the_source_sends(profile_fixture):
    # The same field arrives at second, millisecond, microsecond and nanosecond
    # precision. Casting to timestamp handles all four; a fixed format string would not.
    row = by_profile_id(shape_profile(profile_fixture("hazards")))["hazard-timestamp-precisions"]

    assert row["source_created_at"] is not None
    assert row["source_last_modified_at"] is not None
    assert row["source_last_explicit_update_at"] is not None
    assert row["completion_rate_computed_at"] is not None
    assert row["source_last_modified_at"].year == 2026


def test_completion_details_polymorphism_does_not_affect_the_profile_row(profile_fixture):
    # completionDetails is an object on 3,390 records and an empty array [] on 723. The
    # profile row doesn't read it (silver.whoz_profile_completion_rules explodes it
    # separately), so both forms must shape fine. This guards against someone later
    # adding a struct-typed extraction of it here and reintroducing the hazard.
    rows = by_profile_id(shape_profile(profile_fixture("hazards")))

    assert rows["hazard-completion-details-empty-array"]["profile_id"] is not None
    assert rows["hazard-completion-rate-int"]["profile_id"] is not None


def test_payload_is_not_carried_into_the_output(profile_fixture):
    # Deliberate: AUTO CDC compares whole rows with `<=>` to detect real changes, and
    # VARIANT does not support that comparison — carrying payload here failed the SCD2
    # flow with INVALID_ORDERING_TYPE. Bronze keeps the raw payload forever; join back on
    # profile_id. If this assertion ever fails, that flow is about to break again.
    result = shape_profile(profile_fixture("typical"))

    assert "payload" not in result.columns

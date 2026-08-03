"""Layer 1 — does the transformation code do what we intended? (talent entity)

Unit tests for shape_talent(), the bronze -> silver row shaping behind
silver.whoz_talents and silver.whoz_talent_versions.

The talent export nests a whole profile object under `profile`. shape_talent deliberately
does not re-model it — see whoz_ingestion/shaping/talent.py's header — so most of what these
tests pin down is the *boundary*: which profile fields are lifted, what happens when the
nesting isn't the shape we assumed, and that the talent's own attributes survive intact.
"""

from helpers import to_utc_strings
from pyspark.sql import DataFrame

from whoz_ingestion.shaping.talent import shape_talent


def by_talent_id(df: DataFrame) -> dict:
    """{talent_id: Row} — lets a test address one fixture record by name."""
    return {row["talent_id"]: row for row in df.collect()}


# -------------------------------------------------------------------------------------
# Happy path — the real record from the export, unmodified.
# -------------------------------------------------------------------------------------
def test_typical_records_shape_without_loss(talent_fixture):
    rows = by_talent_id(shape_talent(talent_fixture("typical")))

    assert len(rows) == 2
    row = rows["668be62750f9cf5d75f87e63"]
    assert row["federation_id"] == "668be62550f9cf5d75f87e61"
    assert row["user_id"] == "668be625080dd2051b364775"
    assert row["workspace_id"] == "668be62650f9cf5d75f87e62"
    assert row["permission_scope"] == "SECRET"
    assert row["is_removed"] is False
    assert row["is_end_user"] is True
    assert row["is_freely_assignable"] is False
    assert row["time_entry_preferred_unit"] == "DAY"
    # The embedded profile is lifted as a foreign key plus light attributes, nothing more.
    assert row["profile_id"] == "668be627644a535ac1d1f5fa"
    assert row["profile_talent_id"] == row["talent_id"]
    assert row["profile_is_main_version"] is True
    assert row["profile_status"] == "DRAFT"
    assert row["profile_version_name"] == "Main version"
    assert row["workspace_history_count"] == 1
    assert row["source_file"].endswith(".json")


def test_second_typical_record_covers_the_populated_variant(talent_fixture):
    # The first record is almost entirely empty collections; this one has a real headline,
    # two workspace history entries and an object-form completionDetails, so between them
    # the fixture covers both ends of what the export contains.
    row = by_talent_id(shape_talent(talent_fixture("typical")))["7791ac3861a0d06e86f98f74"]

    assert row["profile_status"] == "VALIDATED"
    assert row["profile_completion_rate"] == 0.42
    assert row["time_entry_preferred_unit"] == "HOUR"
    assert row["workspace_history_count"] == 2
    assert row["qualification_count"] == 1


def test_empty_input_produces_no_rows_not_an_error(talent_bronze):
    assert shape_talent(talent_bronze([])).count() == 0


# -------------------------------------------------------------------------------------
# The nesting hazard — the whole reason profile_container_type exists.
# -------------------------------------------------------------------------------------
def test_single_profile_object_is_reported_as_object(talent_fixture):
    rows = by_talent_id(shape_talent(talent_fixture("typical")))

    assert rows["668be62750f9cf5d75f87e63"]["profile_container_type"] == "OBJECT"


def test_several_profiles_null_the_profile_columns_and_are_detected(talent_fixture):
    # THE case this entity was designed around. A talent id can have more than one profile;
    # today's export nests exactly one, as an object. If that ever becomes an array,
    # try_variant_get("$.profile.id") does NOT fail — it returns NULL, every profile_*
    # column empties, and the pipeline reports a perfectly healthy run.
    #
    # This test pins that real behaviour rather than an aspiration, so it documents what
    # actually happens today, and it proves the sensor sees it. The profile_is_not_an_array
    # expectation is what turns the sensor into an alert (tests/layer3_rules/test_talent_rules.py).
    row = by_talent_id(shape_talent(talent_fixture("violations")))["violation-profile-as-array"]

    assert row["profile_container_type"] == "ARRAY"
    assert row["profile_id"] is None
    assert row["profile_status"] is None
    assert row["profile_completion_rate"] is None
    # The talent's own columns are unaffected — only the nested read goes quiet.
    assert row["user_id"] == "u-profile-as-array"
    assert row["federation_id"] == "fed-1"


def test_absent_profile_is_distinguishable_from_an_array(talent_fixture):
    # A talent with no profile at all must not look like the array case, or the alert on
    # the array case would be drowned by ordinary missing data.
    row = by_talent_id(shape_talent(talent_fixture("violations")))["violation-no-profile"]

    assert row["profile_id"] is None
    assert row["profile_container_type"] != "ARRAY"


# -------------------------------------------------------------------------------------
# Type hazards
# -------------------------------------------------------------------------------------
def test_headline_present_with_every_field_null_still_shapes(talent_fixture):
    # A third variant of `headline` beyond the profile export's absent/partial forms: the
    # object is there with all twelve fields null. shape_talent doesn't read headline, so
    # what this actually pins is that the surrounding record survives it.
    row = by_talent_id(shape_talent(talent_fixture("hazards")))["hazard-headline-all-null"]

    assert row["profile_id"] == "p-headline-all-null"
    assert row["profile_status"] == "DRAFT"


def test_completion_rate_normalizes_int_and_float_to_double(talent_fixture):
    result = shape_talent(talent_fixture("hazards"))
    rows = by_talent_id(result)

    assert rows["hazard-completion-rate-int"]["profile_completion_rate"] == 1.0
    assert dict(result.dtypes)["profile_completion_rate"] == "double"


def test_timestamps_parse_at_every_precision_the_source_sends(talent_fixture):
    # Second, millisecond and nanosecond precision across these three fields, all cast with
    # the same `timestamp` target — a fixed format string would handle at most one of them.
    #
    # Note to_utc_strings: asserting an exact instant is only stable when the rendering
    # happens inside Spark, under the session time zone conftest pins. Collecting the
    # datetime directly would give a different answer here than in CI.
    rows = by_talent_id(to_utc_strings(shape_talent(talent_fixture("hazards"))))
    row = rows["hazard-timestamp-precisions"]

    assert row["source_created_at"] == "2024-01-02 03:04:05.000"          # seconds
    assert row["source_last_modified_at"] == "2026-07-01 12:30:45.123"    # milliseconds
    # Nanoseconds: parsed rather than rejected, truncated to Spark's microsecond precision.
    assert row["last_connection_at"] == "2026-07-01 12:30:45.123"


def test_empty_and_absent_collections_are_counted_differently(talent_fixture):
    # An empty array counts 0; an absent key counts NULL. Worth pinning: a downstream SUM
    # or AVG over these treats the two differently without saying so.
    rows = by_talent_id(shape_talent(talent_fixture("hazards")))

    empty = rows["hazard-empty-collections"]
    assert empty["tag_count"] == 0
    assert empty["workspace_history_count"] == 0
    assert empty["recruitment_stage_date_count"] == 0

    absent = rows["hazard-collections-absent"]
    assert absent["tag_count"] is None
    assert absent["workspace_history_count"] is None
    assert absent["recruitment_stage_date_count"] is None


def test_nested_collection_sizes_reach_into_the_recruitment_object(talent_fixture):
    row = by_talent_id(shape_talent(talent_fixture("hazards")))["hazard-multiple-workspace-history"]

    assert row["workspace_history_count"] == 3
    assert row["recruitment_stage_date_count"] == 0
    assert row["recruitment_workflow_step_date_count"] == 0


def test_sparse_record_nulls_columns_instead_of_failing(talent_fixture):
    row = by_talent_id(shape_talent(talent_fixture("hazards")))["hazard-sparse-record"]

    assert row["profile_id"] == "p-sparse-record"
    assert row["permission_scope"] is None
    assert row["source_created_at"] is None
    assert row["last_connection_at"] is None


def test_payload_is_not_carried_into_the_output(talent_fixture):
    # Same reason as shape_profile: AUTO CDC compares whole rows with `<=>` and VARIANT
    # does not support that comparison (INVALID_ORDERING_TYPE). Join bronze on talent_id
    # for the raw nested profile object.
    assert "payload" not in shape_talent(talent_fixture("typical")).columns

"""shape_profile() run over the whole fixture corpus, rather than hand-built literals.

test_silver_whoz_profile.py builds two-row DataFrames inline to pin one behaviour each.
That is the right shape for "does this expression do what I meant". It cannot answer the
questions that only a corpus can:

  * does the array-root explode actually yield one row per profile, or one row holding
    all of them? (That exact bug reached the first live run — commit b95415e.)
  * do the collection-size columns agree with an independent count, on every record?
  * does anything in a realistic mix of key sets make try_variant_get throw?

Every expected value here is computed from the fixture in plain Python and compared to
what Spark produced. Two independent routes to the same number — a hardcoded expectation
would only prove the fixture hadn't changed, not that the transform is right.

Rows are keyed by talent_id, not profile_id: one fixture record deliberately has no `id`
at all (see fixtures/README.md), and it still needs to be checked.
"""

import pytest
from utilities.profile_shaping import shape_profile


@pytest.fixture(scope="module")
def shaped(bronze_fixture):
    """shape_profile() over the fixture, collected once and reused by every test here."""
    return {row["talent_id"]: row for row in shape_profile(bronze_fixture).collect()}


@pytest.fixture(scope="module")
def expected(fixture_profiles):
    """The same records as plain Python, keyed the same way."""
    return {p["talentId"]: p for p in fixture_profiles}


def test_one_row_per_profile_not_one_row_holding_all_of_them(shaped, fixture_profiles):
    # The load bug this guards against does not raise: a JSON array read without an
    # explode step yields a single row whose payload is the entire array, and every
    # downstream count silently becomes 1. Asserting the row count against the number of
    # array elements is what makes that visible.
    assert len(shaped) == len(fixture_profiles) == 14


def test_every_record_shapes_without_raising(shaped):
    # try_variant_get must null a column rather than fail a batch, across a realistic mix
    # of key sets — including the record whose root keys are a bare minimum.
    assert len(shaped) == 14
    assert all(row is not None for row in shaped.values())


def test_only_the_id_less_record_has_a_null_profile_id(shaped, expected):
    # profile_id_not_null (expect_or_drop) will remove exactly this row in the pipeline,
    # so silver should land 13 of the fixture's 14 records.
    null_ids = {talent_id for talent_id, row in shaped.items() if row["profile_id"] is None}

    assert len(null_ids) == 1
    assert all("id" not in expected[talent_id] for talent_id in null_ids)


def test_completion_rate_matches_python_on_every_record(shaped, expected):
    # int on some records, float on others; both must land as the same double Python read.
    for talent_id, row in shaped.items():
        source = expected[talent_id].get("completionRate")
        if source is None:
            assert row["completion_rate"] is None
        else:
            assert row["completion_rate"] == pytest.approx(float(source)), f"completion_rate for {talent_id}"


@pytest.mark.parametrize(
    ("column", "json_key"),
    [
        ("aptitude_count", "aptitudes"),
        ("position_count", "positions"),
        ("skill_rating_count", "skillRatings"),
        ("qualification_count", "qualificationIds"),
    ],
)
def test_collection_counts_match_python(shaped, expected, column, json_key):
    # Guards the size(cast(payload:x as array<variant>)) expressions — a typo in one of
    # those JSON paths yields NULL, not an error, so nothing else would notice.
    for talent_id, row in shaped.items():
        assert row[column] == len(expected[talent_id].get(json_key, [])), f"{column} for {talent_id}"


def test_headline_absent_and_headline_present_both_yield_null_aim(shaped, expected):
    # aim is present-and-null wherever there is a headline, and the headline is absent
    # entirely elsewhere. Both must flatten to NULL; the distinction survives only on the
    # raw payload in bronze.
    assert any("headline" not in p for p in expected.values())
    assert any("headline" in p for p in expected.values())

    for talent_id, row in shaped.items():
        assert row["headline_aim"] is None, f"headline_aim for {talent_id}"

        headline = expected[talent_id].get("headline") or {}
        assert row["headline_job_title"] == headline.get("jobTitle"), f"headline_job_title for {talent_id}"


def test_all_three_timestamp_precisions_parse(shaped, expected):
    # Second, millisecond AND nanosecond precision appear in the same field in the real
    # export. A fixed format string would parse one and null the other two; casting to
    # timestamp handles all three. Every record has a lastModifiedDate, so a NULL here
    # means a precision the cast silently rejected.
    for talent_id, row in shaped.items():
        assert expected[talent_id].get("lastModifiedDate") is not None
        assert row["source_last_modified_at"] is not None, (
            f"lastModifiedDate {expected[talent_id]['lastModifiedDate']!r} failed to parse for {talent_id}"
        )


def test_sequencing_column_is_populated_on_every_record(shaped):
    # source_last_modified_at is what both AUTO CDC flows sequence_by. A NULL there means
    # a record with no defined ordering against its other versions — worth knowing before
    # it reaches an upsert, not after.
    assert all(row["source_last_modified_at"] is not None for row in shaped.values())


def test_drift_sensor_sees_every_distinct_key_set(bronze_fixture, fixture_profiles):
    # payload_top_level_keys is the cheap early warning for a new or vanished Whoz field.
    # It only works if it really is one row per profile with that profile's own keys.
    distinct = {row["payload_top_level_keys"] for row in bronze_fixture.collect()}
    from_python = {",".join(sorted(p)) for p in fixture_profiles}

    assert distinct == from_python
    assert len(distinct) == 6


def test_payload_is_not_carried_into_silver(bronze_fixture):
    # Bronze keeps the raw VARIANT forever; silver deliberately does not duplicate the
    # single largest column in the dataset. Also the reason both AUTO CDC flows can
    # compare rows at all — VARIANT has no <=> support.
    assert "payload" in bronze_fixture.columns
    assert "payload" not in shape_profile(bronze_fixture).columns


def test_federation_id_is_carried_through_unchanged(shaped, expected):
    for talent_id, row in shaped.items():
        assert row["federation_id"] == expected[talent_id]["federationId"]


def test_lineage_columns_survive_the_transform(bronze_fixture):
    row = shape_profile(bronze_fixture).select("source_file", "ingested_at").first()

    assert row["source_file"].endswith("whoz__profile_report_anonymized.json")
    assert row["ingested_at"] is not None


def test_is_main_version_flags_the_one_non_main_record(shaped, expected):
    # The is_main_version expectation warns rather than drops. This confirms the value it
    # keys on is actually populated — an expectation on a column that is NULL everywhere
    # would pass forever without ever looking at anything.
    non_main = {talent_id for talent_id, row in shaped.items() if row["is_main_version"] is False}

    assert len(non_main) == 1
    assert all(expected[talent_id]["main"] is False for talent_id in non_main)


def test_status_and_language_values_survive(shaped, expected):
    statuses = {row["status"] for row in shaped.values()}
    languages = {row["content_language"] for row in shaped.values()}

    assert statuses == {p["status"] for p in expected.values()}
    assert languages == {p["contentLanguage"] for p in expected.values()}
    assert {"DRAFT", "VALIDATED", "SUBMITTED"} <= statuses


def test_resume_relation_status_keeps_the_source_typo(shaped):
    # RESUME_IMPORT_SUGGESTION_SUBMITED, sic. Documented in PROFILE_COLUMNS' comment.
    # If anyone "fixes" the spelling in the transform, joins against Whoz break silently.
    values = {row["resume_relation_status"] for row in shaped.values()}

    assert "RESUME_IMPORT_SUGGESTION_SUBMITED" in values


def test_shaped_output_column_count_is_stable(bronze_fixture):
    # A column added to shape_profile without a matching line in PROFILE_COLUMNS already
    # fails test_declared_schema_matches_shape_profile_output. This is the cheaper signal
    # that lands first and says plainly what changed.
    assert len(shape_profile(bronze_fixture).columns) == 35


def test_no_column_is_null_on_every_single_record(shaped):
    # A column that is NULL everywhere in a corpus this deliberately varied usually means
    # a wrong JSON path, not genuinely absent data. The fields Whoz has never populated
    # are listed explicitly — anything else joining them should be looked at.
    known_all_null = {
        "headline_aim", "national_mobility", "international_mobility",
        "mobility_date_raw", "mobility_note",
    }
    columns = list(next(iter(shaped.values())).asDict())

    all_null = {c for c in columns if all(row[c] is None for row in shaped.values())}

    assert all_null == known_all_null, f"unexpectedly empty columns: {sorted(all_null - known_all_null)}"

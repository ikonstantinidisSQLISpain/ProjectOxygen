"""Guards fixtures/2020-01-01_whoz__profile_report_anonymized.json against silent rot.

Every other fixture-driven test is only as good as the fixture. If someone tidies up the
JSON and drops the "+22015-07-31" endDate, or makes every completionRate a float, nothing
turns red — the suite just quietly stops proving anything. These tests are what make that
edit fail, loudly, naming the hazard that went missing.

They are pure Python: no SparkSession, no JVM, milliseconds to run. That is deliberate.
The fixture's job is to feed the Spark code path, so the check that it still holds the
right data must not go through that same code path — otherwise a bug in the loader could
hide a hole in the fixture. Two independent routes to the same number.

Every assertion here has a row in fixtures/README.md. Change one, change both.
"""

import json
import re

import pytest
from conftest import FIXTURES_DIR, PROFILE_EXPORT_FIXTURE

# Profiles are addressed by the tail of their id, matching the README table.
COMPLETE = "000001"
EMPTY_COLLECTIONS = "000005"
NO_ID = "000007"
BAD_APTITUDES = "000009"
DANGLING_PARENT = "00000e"


def _suffix(profile: dict) -> str:
    return profile.get("id", "")[-6:]


def _find(profiles: list[dict], suffix: str) -> dict:
    matches = [p for p in profiles if _suffix(p) == suffix]
    assert len(matches) == 1, f"expected exactly one profile ending {suffix}, found {len(matches)}"
    return matches[0]


def _iter_strings(node):
    """Every string value anywhere in the document."""
    if isinstance(node, dict):
        for value in node.values():
            yield from _iter_strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from _iter_strings(value)
    elif isinstance(node, str):
        yield node


# -------------------------------------------------------------------------------------
# File shape — the properties that make this a faithful miniature of the real export
# -------------------------------------------------------------------------------------
def test_file_is_a_pretty_printed_json_array_not_jsonl():
    # The single most important property. The real export is an array root, which is why
    # bronze needs multiLine + singleVariantColumn + variant_explode rather than a plain
    # JSON read. A fixture converted to JSONL would make that whole mechanism untested
    # while every test still passed.
    text = (FIXTURES_DIR / PROFILE_EXPORT_FIXTURE).read_text(encoding="utf-8")

    assert text.lstrip().startswith("["), "root must be a JSON array"
    assert text.rstrip().endswith("]")
    assert text.count("\n") > 100, "must stay pretty-printed across many lines, like the real export"
    assert isinstance(json.loads(text), list)


def test_filename_matches_the_pipeline_glob():
    # bronze_whoz_profiles.py filters the shared landing volume with
    # FILE_NAME_GLOB = "*whoz__profile_report_anonymized.json". A fixture that doesn't
    # match it can't be used to drive a real pipeline run.
    assert PROFILE_EXPORT_FIXTURE.endswith("whoz__profile_report_anonymized.json")
    assert re.match(r"^\d{4}-\d{2}-\d{2}_", PROFILE_EXPORT_FIXTURE), "keep the dated prefix"


def test_counts_match_the_readme(fixture_profiles):
    assert len(fixture_profiles) == 14
    assert sum(1 for p in fixture_profiles if "id" in p) == 13
    assert sum(len(p.get("aptitudes", [])) for p in fixture_profiles) == 6
    assert sum(len(p.get("positions", [])) for p in fixture_profiles) == 7
    assert sum(len(q.get("aptitudeReferences", []))
               for p in fixture_profiles for q in p.get("positions", [])) == 3
    assert sum(len(p.get("skillRatings", [])) for p in fixture_profiles) == 5
    assert sum(len(p["completionDetails"]) for p in fixture_profiles
               if isinstance(p.get("completionDetails"), dict)) == 6


def test_root_key_drift_is_represented(fixture_profiles):
    # The real export has 6 distinct root key sets and 8 optional keys of 32. A fixture
    # where every record has identical keys would never exercise absent-vs-null.
    key_sets = {tuple(sorted(p)) for p in fixture_profiles}

    assert len(key_sets) == 6


# -------------------------------------------------------------------------------------
# The type hazards from docs/whoz_profile_data_model.md, one test each
# -------------------------------------------------------------------------------------
def test_hazard_1_completion_details_is_polymorphic(fixture_profiles):
    # Object on some records, empty array on others. Rules out schema inference entirely.
    forms = {type(p.get("completionDetails")).__name__ for p in fixture_profiles}

    assert forms == {"dict", "list"}
    assert any(p.get("completionDetails") == [] for p in fixture_profiles)


def test_hazard_2_extended_year_end_date_is_present(fixture_profiles):
    # "+22015-07-31" — CAST(... AS DATE) raises on it, try_variant_get returns NULL.
    end_dates = [q.get("endDate") for p in fixture_profiles for q in p.get("positions", [])]

    assert "+22015-07-31" in end_dates


def test_hazard_3_completion_rate_arrives_as_both_int_and_float(fixture_profiles):
    rates = [p.get("completionRate") for p in fixture_profiles if "completionRate" in p]
    # bool is a subclass of int in Python; exclude it so a stray true/false can't pass this.
    ints = [r for r in rates if isinstance(r, int) and not isinstance(r, bool)]
    floats = [r for r in rates if isinstance(r, float)]

    assert ints and floats, f"need both int and float forms, got {sorted(set(map(type, rates)), key=str)}"
    assert all(0 <= r <= 1 for r in rates), "completionRate is a 0-1 fraction, not a percentage"


def test_hazard_3_cumulative_experience_arrives_as_both_int_and_float(fixture_profiles):
    values = [a.get("cumulativeExperience")
              for p in fixture_profiles for a in p.get("aptitudes", [])
              if "cumulativeExperience" in a]

    assert any(isinstance(v, int) and not isinstance(v, bool) for v in values)
    assert any(isinstance(v, float) for v in values)


def test_hazard_4_all_three_timestamp_precisions_are_present(fixture_profiles):
    stamps = [s for s in _iter_strings(fixture_profiles)
              if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", s)]

    seconds = [s for s in stamps if re.match(r"^\S+T\d{2}:\d{2}:\d{2}Z$", s)]
    millis = [s for s in stamps if re.match(r"^\S+T\d{2}:\d{2}:\d{2}\.\d{3}Z$", s)]
    nanos = [s for s in stamps if re.match(r"^\S+T\d{2}:\d{2}:\d{2}\.\d{9}Z$", s)]

    assert seconds, "no second-precision timestamp left in the fixture"
    assert millis, "no millisecond-precision timestamp left in the fixture"
    assert nanos, "no nanosecond-precision timestamp left in the fixture"


def test_hazard_5_absent_is_distinguished_from_null(fixture_profiles):
    # The distinction VARIANT preserves and an inferred struct erases. `hobbies` is
    # *omitted* on most records; `headline.aim` is *present and null*.
    assert any("hobbies" not in p for p in fixture_profiles), "need a record with hobbies absent"
    assert any("hobbies" in p for p in fixture_profiles), "need a record with hobbies present"

    assert any("headline" not in p for p in fixture_profiles), "need a record with headline absent"
    headlines = [p["headline"] for p in fixture_profiles if "headline" in p]
    assert headlines, "need at least one record with a headline"
    assert all("aim" in h and h["aim"] is None for h in headlines), "headline.aim must be present-and-null"


# -------------------------------------------------------------------------------------
# Records that exist to trip a specific pipeline expectation
# -------------------------------------------------------------------------------------
def test_exactly_one_record_has_no_id(fixture_profiles):
    # profile_id_not_null is expect_or_drop: this record and only this record should be
    # dropped from silver, so "13 rows out of 14 in" is a meaningful assertion elsewhere.
    without_id = [p for p in fixture_profiles if "id" not in p]

    assert len(without_id) == 1
    assert without_id[0]["talentId"].endswith(NO_ID)


def test_expectation_tripwires_are_present(fixture_profiles):
    assert any(p.get("main") is False for p in fixture_profiles), "is_main_version needs a false record"
    assert any(p.get("removed") is True for p in fixture_profiles), "is_removed drift needs a true record"

    aptitudes = [a for p in fixture_profiles for a in p.get("aptitudes", [])]
    assert any("id" not in a for a in aptitudes), "aptitude_id_not_null needs an id-less aptitude"
    assert any(a.get("proficiency", 0) > 5 for a in aptitudes), "proficiency_in_range needs an out-of-range value"

    positions = [q for p in fixture_profiles for q in p.get("positions", [])]
    assert any("startDate" in q and "endDate" in q and q["endDate"] < q["startDate"]
               for q in positions), "dates_ordered needs an end-before-start position"


def test_unmodelled_collections_have_a_non_empty_example(fixture_profiles):
    # These are empty on all 4,113 real records. The alarm sketched at the bottom of
    # silver_whoz_profile_children.py fires when they stop being — which needs a record
    # where they aren't.
    assert any(p.get("customFields") for p in fixture_profiles)
    assert any(p.get("schedules") for p in fixture_profiles)


def test_an_unknown_root_key_is_present(fixture_profiles):
    # Feeds the payload_top_level_keys drift sensor: a new Whoz field must show up as a
    # new key set, not as a failure.
    known = {
        "id", "talentId", "federationId", "versionName", "main", "status", "contentLanguage",
        "permissionScope", "travelRange", "removed", "resumeRelationStatus", "completionRate",
        "completionRateLastComputedDate", "createdDate", "createdBy", "lastModifiedDate",
        "lastModifiedBy", "lastExplicitUpdate", "lastExplicitUpdateBy", "hobbies", "headline",
        "completionDetails", "aptitudes", "positions", "skillRatings", "qualificationIds",
        "targetSkills", "customFields", "functionalDomains", "schedules",
        "targetFunctionalDomains", "targetSkillRatings",
    }
    unknown = {k for p in fixture_profiles for k in p} - known

    assert unknown, "no unknown root key left to exercise the drift sensor"


def test_position_hierarchy_has_both_a_resolving_and_a_dangling_parent(fixture_profiles):
    # All 1,197 parentPositionId values resolve in the real export. Both cases are here so
    # a referential-integrity check can be written against a known-good and known-bad row.
    all_ids = {q["id"] for p in fixture_profiles for q in p.get("positions", []) if "id" in q}
    parents = [q["parentPositionId"] for p in fixture_profiles
               for q in p.get("positions", []) if "parentPositionId" in q]

    assert any(parent in all_ids for parent in parents), "need a parentPositionId that resolves"
    assert any(parent not in all_ids for parent in parents), "need a dangling parentPositionId"


# -------------------------------------------------------------------------------------
# Keep it synthetic
# -------------------------------------------------------------------------------------
def test_single_tenant_federation_id(fixture_profiles):
    # One value across the whole export, as in the real file.
    assert len({p["federationId"] for p in fixture_profiles}) == 1


@pytest.mark.parametrize("profile_key", [COMPLETE, EMPTY_COLLECTIONS, BAD_APTITUDES, DANGLING_PARENT])
def test_named_profiles_still_exist(fixture_profiles, profile_key):
    # The README refers to these by id suffix. Renumbering the fixture without updating the
    # docs is the likeliest way for the two to drift apart.
    assert _find(fixture_profiles, profile_key)

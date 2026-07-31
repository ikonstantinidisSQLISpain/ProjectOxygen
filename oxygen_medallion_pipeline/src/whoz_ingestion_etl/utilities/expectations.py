# =====================================================================================
# Data quality rules for the whoz_ingestion_etl pipeline.
#
# Every rule the pipeline enforces lives here as plain data — a {name: SQL predicate}
# dict — instead of being spelled out in a decorator argument in transformations/.
# Two things read these dicts:
#
#   1. the pipeline, via @dp.expect_all / @dp.expect_all_or_drop (transformations/*.py)
#   2. the test suite, via tests/layer3_rules/ (test_rule_hygiene.py plus the per-entity
#      test_profile_rules.py / test_talent_rules.py)
#
# That is the whole point of the indirection. A predicate passed straight to
# @dp.expect_or_drop("name", "sql") is an opaque string: py_compile, ruff, pytest and
# `databricks bundle validate` all pass green on a rule that references a column which
# does not exist or that silently matches nothing. Pulling them here — this module
# imports nothing, so a test can import it without a Spark session or a running
# pipeline — is what lets the tests evaluate every rule against a real DataFrame and
# fail locally instead of at the next pipeline update. Same reasoning as PROFILE_COLUMNS
# in shaping/profile.py.
#
# NAMING CONVENTION — the suffix is the action, and it is a contract, not a label:
#
#   *_MUST_HOLD    -> @dp.expect_all_or_drop. Violating rows are DROPPED. Use only for
#                     rules where a row that fails is unusable: a null primary key.
#   *_SHOULD_HOLD  -> @dp.expect_all. Violating rows are KEPT and counted. Use for
#                     everything else — assumptions about the source that hold today and
#                     should raise an eyebrow, not drop data, if they ever stop.
#
# Write SHOULD_HOLD rules null-safe ("x IS NULL OR <check on x>") unless a NULL is itself
# the thing you want to hear about. A NULL predicate result is not TRUE, so it counts as a
# failure; a range check that is not null-safe therefore flags every row with a missing
# optional field, and the signal drowns. Two deliberate exceptions below —
# talent_id_not_null and is_main_version — where a NULL genuinely is the signal.
#
# To add a rule: add one line to the right dict below. The pipeline picks it up with no
# change to transformations/, and tests/layer3_rules/test_rule_hygiene.py starts checking
# that it parses and resolves automatically. Adding the case that proves it catches
# something is still on you — that goes in the entity's behaviour file,
# tests/layer3_rules/test_profile_rules.py or test_talent_rules.py.
# =====================================================================================

# -------------------------------------------------------------------------------------
# whoz_profile_shaped -> silver.whoz_profiles / silver.whoz_profile_history
# Applied to the shaped view, so these run once and protect both AUTO CDC targets.
# -------------------------------------------------------------------------------------
# profile_id is the key both AUTO CDC flows merge on. A NULL one cannot be upserted, so
# this is the one rule in the profile path that drops rather than warns. PROFILE_COLUMNS
# declares profile_id STRING NOT NULL on the strength of it.
PROFILE_MUST_HOLD = {
    "profile_id_not_null": "profile_id IS NOT NULL",
}

PROFILE_SHOULD_HOLD = {
    # Non-null on every record in the analysed export. Warn if that changes: a profile
    # with no talent is still ingestable, it just cannot be joined to anything.
    "talent_id_not_null": "talent_id IS NOT NULL",
    # The source is unversioned in this extract (main=true everywhere), but the model
    # allows several versions per talent. If this starts firing, the grain of
    # silver.whoz_profiles is wrong — one row per profile_id stops meaning one row per
    # talent, and the AUTO CDC keys need revisiting.
    "is_main_version": "is_main_version = true",
    # completionRate is documented as a 0-1 fraction, not a 0-100 percentage (verified
    # against the live export: min 0.0, max 1.0). A value above 1 means Whoz changed the
    # unit, which would silently break any downstream metric built on it.
    "completion_rate_is_fraction": "completion_rate IS NULL OR completion_rate BETWEEN 0 AND 1",
}

# -------------------------------------------------------------------------------------
# whoz_talent_shaped -> silver.whoz_talents / silver.whoz_talent_versions
# -------------------------------------------------------------------------------------
# Named talent_pk_not_null, not talent_id_not_null: PROFILE_SHOULD_HOLD already uses that
# name for the *foreign* key on the profile row, and rule names have to be unique across
# the project (test_rule_names_are_unique_across_the_project) because they surface as
# metric labels in the quality dashboard, where two identically-named rules on different
# tables read as one.
TALENT_MUST_HOLD = {
    "talent_pk_not_null": "talent_id IS NOT NULL",
}

TALENT_SHOULD_HOLD = {
    # THE important one. The talent export nests a single `profile` object, and
    # shape_talent reads it with try_variant_get("$.profile.id") etc. If Whoz ever sends
    # several profiles per talent — an ARRAY where an OBJECT used to be — none of that
    # fails: every profile_* column silently becomes NULL, the pipeline reports a healthy
    # run, and silver fills with talents that look like they have no profile. Verified
    # against both forms on a real session; it really is silent.
    #
    # Written as "not ARRAY" rather than "is OBJECT" on purpose, so it stays quiet for a
    # talent with no profile at all (that case is talent_has_a_profile's job, below) and
    # fires only on the shape change it exists to catch.
    "profile_is_not_an_array": (
        "profile_container_type IS NULL OR profile_container_type <> 'ARRAY'"
    ),
    # The symptom, watched from the other side. Cheap, and it also covers the ordinary
    # case of a talent record arriving with no profile at all. If this turns out to fire
    # broadly on real data, that is information — a talent who never built a profile is
    # plausible, and this is warn-level precisely because that has not been confirmed yet.
    "talent_has_a_profile": "profile_id IS NOT NULL",
    # The embedded profile repeats its parent's talentId. Disagreement means the payload
    # is internally inconsistent, which would make the profile_id join untrustworthy.
    "embedded_talent_id_agrees": (
        "profile_talent_id IS NULL OR profile_talent_id = talent_id"
    ),
    # Same unit hazard as the profile export's completionRate: a 0-1 fraction, not a
    # 0-100 percentage.
    "profile_completion_rate_is_fraction": (
        "profile_completion_rate IS NULL OR profile_completion_rate BETWEEN 0 AND 1"
    ),
    # A login before the account existed means the two timestamps are not what we think
    # they are — most likely a time zone or precision bug at the source.
    "last_connection_not_before_created": (
        "last_connection_at IS NULL OR source_created_at IS NULL "
        "OR last_connection_at >= source_created_at"
    ),
}

# -------------------------------------------------------------------------------------
# silver.whoz_talent_workspace_history — the source's own history[] array.
# -------------------------------------------------------------------------------------
WORKSPACE_HISTORY_MUST_HOLD = {
    "workspace_history_keys_not_null": "talent_id IS NOT NULL AND workspace_id IS NOT NULL",
}

WORKSPACE_HISTORY_SHOULD_HOLD = {
    "since_date_parsed": "since_raw IS NULL OR since_date IS NOT NULL",
}

# -------------------------------------------------------------------------------------
# silver.whoz_profile_aptitudes
# -------------------------------------------------------------------------------------
APTITUDE_MUST_HOLD = {
    "aptitude_id_not_null": "aptitude_id IS NOT NULL",
}

APTITUDE_SHOULD_HOLD = {
    "proficiency_in_range": "proficiency IS NULL OR proficiency BETWEEN 0 AND 5",
    # Each aptitude object repeats its parent's profileId. Disagreement between that and
    # the row we exploded it out of would mean the payload is internally inconsistent.
    "embedded_profile_id_agrees": (
        "embedded_profile_id IS NULL OR embedded_profile_id = profile_id"
    ),
}

# -------------------------------------------------------------------------------------
# silver.whoz_profile_positions
# -------------------------------------------------------------------------------------
POSITION_MUST_HOLD = {
    "position_id_not_null": "position_id IS NOT NULL",
}

POSITION_SHOULD_HOLD = {
    # Catches the "+22015-07-31" extended-year value documented in
    # docs/whoz_profile_data_model.md: end_date comes back NULL from the cast while
    # end_date_raw still holds the string. Warn, don't drop — the rest of the row is fine.
    "end_date_parsed": "end_date_raw IS NULL OR end_date IS NOT NULL",
    "start_date_parsed": "start_date_raw IS NULL OR start_date IS NOT NULL",
    "dates_ordered": "start_date IS NULL OR end_date IS NULL OR end_date >= start_date",
}

# -------------------------------------------------------------------------------------
# silver.whoz_position_aptitude_refs — no natural id, the key is the pair.
# -------------------------------------------------------------------------------------
APTITUDE_REF_MUST_HOLD = {
    "keys_not_null": "position_id IS NOT NULL AND aptitude_id IS NOT NULL",
}

# -------------------------------------------------------------------------------------
# Registry of every rule set above, keyed by "<dataset>.<action>".
#
# Only tests use this: it is what lets tests/layer3_rules/test_rule_hygiene.py assert things about
# ALL rules in the project (they parse, names are unique and snake_case, MUST_HOLD stays
# minimal) without anyone remembering to add the new rule to a list. The pipeline
# imports the individual dicts, never this.
#
# This dict IS maintained by hand, so a new rule set added above and forgotten here would
# be covered by nothing. test_every_rule_set_is_registered fails when that happens — every
# module-level *_MUST_HOLD / *_SHOULD_HOLD dict has to appear below.
# -------------------------------------------------------------------------------------
ALL_RULE_SETS: dict[str, dict[str, str]] = {
    "whoz_profile_shaped.must_hold": PROFILE_MUST_HOLD,
    "whoz_profile_shaped.should_hold": PROFILE_SHOULD_HOLD,
    "whoz_talent_shaped.must_hold": TALENT_MUST_HOLD,
    "whoz_talent_shaped.should_hold": TALENT_SHOULD_HOLD,
    "whoz_talent_workspace_history.must_hold": WORKSPACE_HISTORY_MUST_HOLD,
    "whoz_talent_workspace_history.should_hold": WORKSPACE_HISTORY_SHOULD_HOLD,
    "whoz_profile_aptitudes.must_hold": APTITUDE_MUST_HOLD,
    "whoz_profile_aptitudes.should_hold": APTITUDE_SHOULD_HOLD,
    "whoz_profile_positions.must_hold": POSITION_MUST_HOLD,
    "whoz_profile_positions.should_hold": POSITION_SHOULD_HOLD,
    "whoz_position_aptitude_refs.must_hold": APTITUDE_REF_MUST_HOLD,
}

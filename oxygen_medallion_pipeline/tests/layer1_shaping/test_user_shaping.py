"""Layer 1 — does shape_user() do what we intended? (user entity)

One test per hazard documented in docs/whoz_user_data_model.md §2-3. Every number asserted
here was measured against the export that landed 2026-07-27; the doc records which.

Timestamps go through helpers.to_utc_strings() before being collected — see that function's
docstring for why a collected datetime is machine-dependent and this is not.
"""

import pytest
from helpers import to_utc_strings

from whoz_ingestion.shaping.user import shape_user


@pytest.fixture()
def shaped(user_fixture):
    """fixtures/whoz_users/<name>.json -> {user_id: row-as-dict}, timestamps as strings."""

    def _shape(name):
        result = to_utc_strings(shape_user(user_fixture(name)))
        return {row["user_id"]: row.asDict() for row in result.collect()}

    return _shape


# -------------------------------------------------------------------------------------
# The ordinary case
# -------------------------------------------------------------------------------------
def test_typical_record_is_flattened_as_declared(shaped):
    row = shaped("typical")["668c03f4080dd2051b3647b4"]

    assert row["username"] == "drpcjmr6@mail.com"
    assert row["is_enabled"] is True
    assert row["is_removed"] is False
    assert row["language"] == "en-GB"
    assert row["idp_id"] == "3e1f554b-e64e-45b0-9c70-4ee12e17cb62"
    assert row["source_created_at"] == "2024-07-08 15:21:24.511"
    assert row["last_connection_at"] == "2026-04-02 00:50:51.972"


def test_payload_is_not_carried_into_the_output(user_fixture):
    # AUTO CDC compares whole rows with `<=>` to detect real changes, and VARIANT does not
    # support that comparison — it fails the flow with INVALID_ORDERING_TYPE. Not selecting
    # the column is what prevents it; this test is what stops someone adding it back.
    assert "payload" not in shape_user(user_fixture("typical")).columns


# -------------------------------------------------------------------------------------
# The federation flattening — legitimate only while there is exactly one entry
# -------------------------------------------------------------------------------------
def test_federation_is_flattened_out_of_the_map(shaped):
    rows = shaped("typical")

    admin = rows["668c03f4080dd2051b3647b4"]
    assert admin["federation_id"] == "668be62550f9cf5d75f87e61"
    assert admin["federation_role"] == "ADMIN"
    assert admin["federation_count"] == 1

    member = rows["6a2f7c1e4b8d9f0a3c5e6b71"]
    assert member["federation_role"] == "MEMBER"


# -------------------------------------------------------------------------------------
# The type hazards, one test each
# -------------------------------------------------------------------------------------
def test_empty_array_form_of_a_map_counts_as_zero_not_null(shaped):
    # workspaceRoles and federationRoles are a MAP when populated and an empty ARRAY when not.
    # The array form means "no memberships", which is 0 — a real fact, distinct from "the
    # export did not say", which is the NULL asserted in the sparse test below.
    row = shaped("hazards")["hazard-empty-collections"]

    assert row["federation_count"] == 0
    assert row["workspace_role_count"] == 0
    assert row["former_username_count"] == 0
    assert row["agentic_studio_role_count"] == 0
    assert row["federation_id"] is None
    assert row["federation_role"] is None


def test_absent_keys_read_as_null(shaped):
    # 1,691 of 4,036 records omit idpId and lastConnectionDate entirely rather than sending
    # them as null. try_variant_get returns NULL for both, so the two are indistinguishable
    # downstream — a known, tested equivalence rather than a surprise.
    row = shaped("hazards")["hazard-collections-absent"]

    assert row["idp_id"] is None
    assert row["last_connection_at"] is None


def test_second_and_millisecond_precision_both_parse(shaped):
    # Both precisions occur in the same file. Casting to timestamp handles both; a fixed
    # format string would handle one and silently NULL the other.
    row = shaped("hazards")["hazard-timestamp-precisions"]

    assert row["source_created_at"] == "2024-03-01 08:00:00.000"
    assert row["last_connection_at"] == "2026-01-02 03:04:05.000"
    assert row["source_last_modified_at"] == "2026-01-02 03:04:05.000"


def test_literal_null_string_is_normalised_to_a_real_null(shaped):
    # The source sends the four-character STRING "null" on 983 of 4,036 records. Left alone it
    # is a sentinel that joins to nothing and reads as a real id in every GROUP BY. This is the
    # one value shape_user normalises.
    row = shaped("hazards")["hazard-last-modified-by-null-string"]

    assert row["source_last_modified_by"] is None


def test_created_by_is_not_normalised(shaped):
    # createdBy does not carry the sentinel (0 occurrences measured), so it is passed through
    # untouched. Asserted so that a future "tidy up" does not apply NULLIF to both by symmetry.
    row = shaped("hazards")["hazard-last-modified-by-null-string"]

    assert row["source_created_by"] == "668be625080dd2051b364775"


def test_former_usernames_are_counted_not_carried(shaped):
    # The values are previous login addresses; one of them in the real export encodes a GDPR
    # erasure request. Only the count reaches silver — see docs/whoz_user_data_model.md §3.
    row = shaped("hazards")["hazard-many-former-usernames"]

    assert row["former_username_count"] == 3
    assert "former_usernames" not in row
    assert not any("gdpr" in str(v).lower() for v in row.values())


def test_multiple_workspace_memberships_are_counted(shaped):
    row = shaped("hazards")["hazard-rare-workspace-roles"]

    assert row["workspace_role_count"] == 3


def test_sparse_record_yields_null_counts_not_zero(shaped):
    # The least informative record: no collection keys at all. NULL rather than 0, and the
    # difference is load-bearing — "belongs to no workspaces" and "the export did not say" are
    # different facts, and only one of them should be summed downstream.
    row = shaped("hazards")["hazard-sparse-record"]

    assert row["federation_count"] is None
    assert row["workspace_role_count"] is None
    assert row["former_username_count"] is None
    assert row["agentic_studio_role_count"] is None
    assert row["idp_id"] is None
    assert row["language"] is None

# Whoz user export — data model analysis

Step 0 of [`adding_a_source_entity.md`](adding_a_source_entity.md), for
`whoz__user_report_anonymized.json`. Every cast, schema column and quality check in the
`user` entity is downstream of a measurement here, so this document is the justification for
all of them. **Read it before changing any cast.**

Measured against the file that landed **2026-07-27** in
`/Volumes/oxygen_dev/landing/source/` (5.5 MB, 4,036 records). Counts below are from that
file; re-measure before trusting them against a much later export.

---

## 1. Root shape, grain and keys

| Question | Answer |
|---|---|
| Root shape | Pretty-printed JSON **array**, one object per user — same as the profile and talent exports, so bronze needs `multiLine` + `singleVariantColumn` + an explicit `variant_explode` |
| Grain | One row per Whoz **user account** |
| Records | 4,036 |
| Primary key | `id` — 24-hex ObjectId, 4,035 distinct, **no duplicates** |
| Sequencing field | `lastModifiedDate` — present and non-null on every real record. AUTO CDC is safe |
| Foreign key | **`talent.userId` → `user.id`**, verified: all 2,275 distinct `userId` values in the talent export resolve to a user in this file, **0 orphans** |

### A user is not a talent

`silver.whoz_talents` is a *person in a workspace*; this is the **login account**. The two are
not interchangeable and the cardinality is not 1:1:

* 2,275 of 4,116 talents carry a `userId`; the other 1,841 have none.
* **1,760 of 4,036 users have no talent pointing at them** — service and administrative
  accounts that never became a person in the product.

So a join from users to talents is an outer join in both directions, and any headcount metric
built on `silver.whoz_users` is counting *accounts*, not people. The talent export remains the
place to count people.

### The one all-null record  ·  *this is why the key check is `error`*

Exactly **one** record has `id: null` — and every other field on it is null too. It carries
all 16 keys, all with null values. It is not a partial record; it is an empty one.

This is the whole justification for `user_id_not_null` at `criticality: error`: AUTO CDC
cannot upsert a null key, and the row carries no information to preserve. It is quarantined
and excluded from `silver.whoz_users`. **Expect exactly 1 row in
`silver.whoz_users_quarantine` with `_errors IS NOT NULL` on every full refresh of this
export** — that is correct behaviour, not a bug to chase.

---

## 2. Type hazards

Each of these is a record in `fixtures/whoz_users/hazards.json`, and most are a `warn` check.

### 2a. `workspaceRoles` and `federationRoles` are object-or-array  ·  *the big one*

Both fields arrive as a **map keyed by id** when populated, and as an **empty JSON array
`[]`** when not. Measured:

| Field | object form | array form | array form always empty? |
|---|---|---|---|
| `workspaceRoles` | 2,858 records | 1,178 records | **yes**, every one |
| `federationRoles` | 2,863 records | 1,173 records | **yes**, every one |

This is precisely the `completionDetails` polymorphism documented for the profile export, and
it gets the same treatment: `variant_explode` on the object form yields key/value pairs, and
on the empty-array form yields **no rows at all**, which is exactly what we want. No filter or
type test is needed in the query — the `WHERE key IS NOT NULL` guard the completion-rules
query carries is there for the same reason and is kept here.

The map key is always the id: **0 mismatches** between the `workspaceRoles` key and its
value's own `workspaceId` across all 15,258 entries. `embedded_workspace_id_agrees` watches
that it stays true.

### 2b. `lastModifiedBy` carries the literal string `"null"`

**983 of 4,036 records** have `lastModifiedBy` set to the four-character string `"null"` — not
JSON `null`, which appears exactly once (the empty record). A further 3,052 hold a real 24-hex
id.

Left alone this is a sentinel that joins to nothing and reads as a real value in every
`GROUP BY`. `shape_user` maps it to a true `NULL` with `NULLIF`, and the shaping test pins
that. This is the one place the user entity normalises a value rather than passing it through,
and it is deliberate: the alternative is every downstream consumer rediscovering it.

`createdBy` does **not** have this problem — 13 distinct values, all 24-hex, one JSON null.

### 2c. Timestamp precisions: millisecond *and* second

Both formats are present in the same file, in every timestamp column:

| Field | `...sssZ` (milli) | `...ssZ` (second) | null |
|---|---|---|---|
| `createdDate` | 4,031 | 4 | 1 |
| `lastModifiedDate` | 4,029 | 6 | 1 |
| `lastConnectionDate` | 2,330 | 4 | 1 (+1,701 absent) |

Cast to `timestamp`; do **not** parse with a format string, which handles at most one of them
and silently NULLs the other.

### 2d. Absent vs. present-but-null keys

Three distinct top-level key sets, which is what `bronze.whoz_users_payload_shapes` exists to
surface:

| Key set | Records | Missing |
|---|---|---|
| 16 keys | 2,335 | — |
| 14 keys | 1,691 | `idpId`, `lastConnectionDate` |
| 15 keys | 10 | `lastConnectionDate` |

`try_variant_get` returns NULL for an absent path and for a present-null one alike, so this
costs nothing downstream — but it means **"never logged in" and "no `lastConnectionDate` key"
are indistinguishable in silver**, and the hazards fixture carries one of each so that stays a
known, tested equivalence rather than a surprise.

---

## 3. Nested collections, and what each becomes

The runbook requires a decision per array. All four, with reasoning:

| Collection | Shape | Decision |
|---|---|---|
| `workspaceRoles` | map of 1–33 entries; 15,258 total | **Child table** `silver.whoz_user_workspace_roles` |
| `federationRoles` | map, **always exactly 1 entry** | **Flattened onto the user row** |
| `formerUsernames` | array of strings, 0–29, 1,975 total | **Count column only** — values not modelled |
| `agenticStudioRoles` | array, **always empty** | **Not modelled**, watched by a check |

### `workspaceRoles` → a child table, exploded to one row per role

15,258 memberships over 2,858 users, 1–33 workspaces each. Value keys are `workspaceId`,
`workspaceExternalId` and `roles`.

* `workspaceExternalId` is **NULL on all 15,258 entries.** Carried anyway, because a column
  that is always null is cheap and its first non-null value is information.
* `roles` is an array that today holds **exactly one element, on every one of the 15,258
  entries.** The table is nonetheless exploded to one row per **(user, workspace, role)**
  rather than lifting `roles[0]` into a scalar. If Whoz ever grants two roles on one
  membership, the exploded form gains a row and nothing is lost; the scalar form would
  silently drop the second. Same reasoning as the profile export's `completionDetails`: a new
  value should show up as new *rows*, not as a schema change or a silent truncation.
* Roles observed: `COLLABORATOR` 14,751 · `ADMIN` 342 · `STANDARD` 96 · `ADVANCED` 61 ·
  `RESTRICTED` 8. `workspace_role_is_known` warns on anything outside that set.
* 34 distinct workspaces here against 32 in the talent export; all 32 talent workspaces appear
  in this file.

### `federationRoles` → flattened, and the assumption is watched

Every one of the 2,863 populated records has **exactly one** federation entry, holding
**exactly one** role, and the whole file references **a single `federationId`**. A child table
for a strictly 1:1 relationship would be three objects and a join to buy nothing.

So `federation_id` and `federation_role` (`MEMBER` 2,846 · `ADMIN` 17) are columns on
`silver.whoz_users`, alongside `federation_count`. **`federation_count_at_most_one` is a
`warn` check, and it is the tripwire**: the day a user belongs to two federations, that check
fires, and this decision has to be revisited into a proper child table. Written down here so
the reversal is a known move rather than an archaeology exercise.

### `formerUsernames` → count only, deliberately

991 users have at least one, up to 29, 1,975 in total. The values are **previous login
addresses** — the same class of data as `username`, which is an email.

Modelled as `former_username_count` and nothing else. The analytically useful fact is "this
account has been renamed N times"; the addresses themselves have no identified consumer, and
putting a person's historical email addresses into a silver table widens PII exposure for no
benefit. **Reversible without a re-ingest**: the raw array stays in the bronze VARIANT payload
forever, so a child table can be built the day someone has a use for it.

### `agenticStudioRoles` → not modelled, but watched

Present on every record and **empty on every record**. Rather than build a table for it, the
shaped view carries `agentic_studio_role_count` and
`agentic_studio_roles_still_empty` warns the first time Whoz starts populating it. This is the
pattern the profile export's header *describes* but never implemented; it is implemented here
because the column costs one `size()` call.

---

## 4. Low-cardinality fields

| Field | Values | Note |
|---|---|---|
| `enabled` | `true` / `false` | Whether the account can log in |
| `removed` | **`false` on all 4,036** | Soft-delete flag. `user_not_removed` warns if that changes, because a removed account in `silver.whoz_users` changes what the table's grain means |
| `theme` | **`LIGHT` on all 4,036** | UI preference. Carried, not checked — a second theme is not a data-quality event |
| `language` | `en-GB`, `en-US`, `fr-FR` | Not checked: a fourth locale is expected, not suspicious |

The distinction between `removed` and `theme` is the criticality contract in miniature. Both
are single-valued today; only one of them changes the meaning of the table if that stops being
true.

---

## 5. What this entity deliberately does not model

Written down so the decision is visible six months from now, to the standard
`shaping/talent.py`'s header sets:

1. **`formerUsernames` values** — count only. See §3. Reversible from bronze.
2. **`agenticStudioRoles`** — always empty. Count only, with a check that watches it.
3. **`workspaceExternalId`** — carried but always NULL; not used to join anything.
4. **The user↔talent relationship is not materialised here.** `silver.whoz_users` carries no
   `talent_id`. The FK lives on the talent side (`talent.userId`) and that is where it stays —
   inventing a reverse column would mean this table had to be rebuilt whenever a talent
   appeared, and 44% of users have no talent at all.

---

## 6. Resulting tables

| Layer | Table | Grain |
|---|---|---|
| bronze | `bronze.whoz_users` | one row per user, full JSON in a VARIANT `payload` |
| bronze | `bronze.whoz_users_payload_shapes` | one row per distinct top-level key set (drift monitor) |
| silver | `silver.whoz_users` | one row per user, current state — AUTO CDC (SCD1) by `user_id` |
| silver | `silver.whoz_user_versions` | one row per (user, version), `__START_AT`/`__END_AT` — AUTO CDC (SCD2) |
| silver | `silver.whoz_user_workspace_roles` | (user, workspace, role) |
| silver | `silver.whoz_users_quarantine` | rows any DQX check fired on |
| silver | `silver.whoz_user_workspace_roles_quarantine` | ditto, for the child table |

Naming follows the talent entity: `whoz_user_versions` is the SCD2 history of the *record*.
There is no second "history" concept on this entity, so the ambiguity that forced
`whoz_talent_workspace_history`'s careful naming does not arise here.

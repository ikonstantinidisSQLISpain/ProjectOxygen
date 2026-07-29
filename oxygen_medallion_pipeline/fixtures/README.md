# Test fixtures

## `2020-01-01_whoz__profile_report_anonymized.json`

14 **entirely synthetic** profiles — no Whoz data, real or anonymized, is in this file.
Every id, name, company and date was invented for this fixture.

It is a deliberate miniature of the real export: a pretty-printed JSON **array** root
(not JSONL), the same field names, and one instance of every type hazard catalogued in
`docs/whoz_profile_data_model.md`. The real export is 101.6 MB / 4,113 records; this is
the smallest file that still breaks everything the big one breaks.

The filename matches the pipeline's `FILE_NAME_GLOB`
(`*whoz__profile_report_anonymized.json`, see `src/whoz_ingestion_etl/transformations/bronze_whoz_profiles.py`)
including the `YYYY-MM-DD_` generation-date prefix, so the same file can be dropped into
a landing volume to drive a real pipeline run. The date is 2020-01-01 so it is obvious at
a glance that this is not a real export.

### What each profile is for

JSON has no comments, so the map lives here. Ids end in the suffix shown.

| Profile | Encodes |
|---|---|
| `…000001` | The "complete" record: every root key present, full `headline`, `completionDetails` as an **object**, float `completionRate`, 3 aptitudes (one with **no `conceptId`**, int *and* float `cumulativeExperience`, all three `visibility` values), 2 positions in a **parent → child mission hierarchy**, `aptitudeReferences`, `qualificationIds` |
| `…000002` | `completionRate` as **int `1`** · `completionDetails` as an **empty array `[]`** · `headline` **entirely absent** · `hobbies` absent · second-precision timestamps · empty `positions[]` |
| `…000003` | `headline` present with `aim: null` and all five mobility fields null · `seekingOpportunities` non-null (`false`) · **nanosecond**-precision timestamps · `completionRate` int `0` |
| `…000004` | **`endDate: "+22015-07-31"`** — the extended-year date that `CAST(… AS DATE)` raises on · the source enum typo `RESUME_IMPORT_SUGGESTION_SUBMITED` |
| `…000005` | Minimal key set: every collection empty, no optional root keys. The "nothing to see here" record |
| `…000006` | `skillRatings[]` populated, 4 of 5 at rating `0` (the legacy-field shape) · `CREATED_FROM_RESUME` |
| `…000007` | **No `id` at all** — the only record that `profile_id_not_null` (`expect_or_drop`) should drop. Everything else in the file must survive |
| `…000008` | `positions[]` key drift: one with `endDate` and no `startDate`, one with **`endDate` < `startDate`** (`dates_ordered`), one with no `endDate` and no `qualificationIds` |
| `…000009` | `proficiency: 7` — **out of the 0–5 range** (`proficiency_in_range`) · a second aptitude with **no `id`** (`aptitude_id_not_null`, `expect_or_drop`) |
| `…00000a` | `main: false` and a different `versionName` — the only record that trips the `is_main_version` expectation |
| `…00000b` | `removed: true` — the doc says `false` on every real record; this is the drift sensor |
| `…00000c` | `customFields` and `schedules` **non-empty** — the "unmodelled collections started filling up" alarm sketched at the bottom of `silver_whoz_profile_children.py` |
| `…00000d` | An **unknown new root key** (`experimentalScore`) — should appear as a new `payload_top_level_keys` value, not as a failure |
| `…00000e` | A `parentPositionId` that **resolves to nothing** in the file — real export has 0 of these, so it is the referential-integrity canary |

### Counts to assert against

Derived from the file itself, not hand-tallied — if you change the fixture, rerun
`tests/test_fixture_integrity.py`, which recomputes and re-checks all of these.

| | |
|---|---|
| profiles (array elements) | 14 |
| …with a non-null `id` | 13 |
| aptitudes | 6 |
| …with a non-null `id` | 5 |
| positions | 7 |
| `aptitudeReferences` | 3 |
| `skillRatings` | 5 |
| completion rules (`completionDetails` as object) | 6 across 2 profiles |
| distinct root key sets | 6 |

### Adding to it

Add a row to the table above and a case to `tests/test_fixture_integrity.py` in the same
commit. A fixture nobody can explain is worse than no fixture — the point of this file is
that every record is here for a stated reason.

Keep it synthetic. This file is committed to git and mirrored to every developer's
machine; the real export is not anonymized in `positions[].employerName` (see
`docs/whoz_profile_data_model.md`) and must never be pasted in here.

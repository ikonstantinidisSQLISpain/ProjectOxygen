# Whoz profile export — data model analysis

Source: `whoz__profile_report_anonymized.json` — 101.6 MB, 4,113 records.

## File shape

The root is a **JSON array**, pretty-printed across 2.76 M lines. It is *not* JSONL.
Any reader must use `multiLine = true`; Spark then yields one row per array element.

Each element is one **profile** (a Whoz "profile version"). In this extract:

- `id` — 4,113 distinct values, unique → primary key
- `talentId` — 4,113 distinct values, 1 profile per talent
- `main` — `true` on every record; `versionName` = "Main version" everywhere
- `federationId` — a single value on all 4,113 records (single tenant)

So although the model *supports* multiple versions per talent, this extract contains
only the main version. Don't assume `talentId` is unique forever — key on `id`.

## Entity graph

```
profile (4,113)                          PK id, FK talentId, federationId
├── headline                     0..1    embedded object, present on 3,069
├── completionDetails            0..1    MAP<rule_name, {satisfied, weight}> — 13 rules
├── aptitudes[]               113,810    PK id  → skills/languages/tools
├── positions[]                15,104    PK id  → jobs & missions
│   ├── aptitudeReferences[]  137,050    skills claimed on that position (no own id)
│   └── qualificationIds[]        586    string ids
├── skillRatings[]              6,792    legacy — see below
├── qualificationIds[]            423    string ids
└── targetSkills[]                  2    effectively unused
```

Referential integrity is clean — checked across all records:

- `aptitudes[].profileId` == parent `id` — 0 mismatches
- `aptitudes[].talentId` == parent `talentId` — 0 mismatches
- `positions[].profileId` == parent `id` — 0 mismatches
- `positions[].parentPositionId` → 1,197 distinct values, **all** resolve to a
  `positions[].id` within the file. Positions are self-hierarchical: a parent
  position (the job) with child positions (the missions under it).

## Field inventory

### Profile root (32 keys)

| Field | Type | Present | Notes |
|---|---|---|---|
| `id`, `talentId`, `federationId` | string | 4,113 | Mongo-style 24-hex ObjectIds |
| `versionName`, `main` | string, bool | 4,113 | constant in this extract |
| `contentLanguage` | string | 4,113 | en 2,861 · fr 1,195 · nl 27 · it 13 · de 10 · es 7 |
| `status` | string | 4,113 | DRAFT 3,904 · VALIDATED 111 · SUBMITTED 98 |
| `permissionScope` | string | 4,113 | SECRET on every row |
| `travelRange` | string | 4,113 | DEFAULT on every row |
| `removed` | bool | 4,113 | `false` on every row |
| `completionRate` | **int or float** | 4,113 | 0–1, a fraction not a percentage (0.42 = 42%); see type hazards |
| `completionDetails` | **object or array** | 4,113 | see type hazards |
| `completionRateLastComputedDate` | string | 3,390 | ISO-8601 |
| `createdDate`, `lastModifiedDate` | string | 4,113 | ISO-8601 |
| `lastExplicitUpdate` | string | 3,811 | ISO-8601 |
| `createdBy`, `lastModifiedBy`, `lastExplicitUpdateBy` | string | 3,088 | user ObjectIds |
| `headline` | object | 3,069 | |
| `hobbies` | string | 2,044 | free text |
| `resumeRelationStatus` | string | 1,387 | RESUME_IMPORT_SUGGESTION_NOT_PROCESSED 1,252 · CREATED_FROM_RESUME 90 · RESUME_IMPORT_SUGGESTION_SUBMITED 45 (note the typo in the enum value) |
| `aptitudes`, `positions`, `skillRatings`, `qualificationIds`, `targetSkills` | array | 4,113 | populated |
| `customFields`, `functionalDomains`, `schedules`, `targetFunctionalDomains`, `targetSkillRatings` | array | 4,113 | **empty on every single record** |

### `aptitudes[]` — 113,810 rows

The skills inventory. `id` is unique across the whole file → usable as PK.

| Field | Present / 113,810 | Notes |
|---|---|---|
| `id`, `name`, `type`, `profileId`, `talentId` | 113,810 | always present |
| `conceptId` | 111,405 (97.9%) | UUID into a skill taxonomy — 6,626 distinct concepts vs **13,222 distinct names**. The 2,405 rows without a `conceptId` are free-text skills not yet mapped to the taxonomy. |
| `visibility` | 112,230 | REGULAR 95,565 · FAVORITE 15,389 · HIDDEN 1,276 |
| `proficiency` | 110,206 | int 0–5 (4 is most common, then 3, then 0) |
| `cumulativeExperience` | 113,354 | years, 0–13.83 — **int or float** |
| `augmentedWithAi` | 82,890 (73%) | `true` on only 171 rows |

`type` (11+ values): FUNCTIONAL 25,395 · TECHNICAL 20,935 · TOOL 16,988 ·
FRAMEWORK 9,931 · PROGRAMMING_LANGUAGE 8,749 · PLATFORM 6,334 · LANGUAGE 5,126 ·
METHOD 4,695 · BUSINESS_SOFTWARE 3,827 · DATABASE 2,957 · CROSS_FUNCTIONAL …

Distribution is heavily skewed: 2,134 of 4,113 profiles have at least one aptitude,
average 27.7, **max 373**.

### `positions[]` — 15,104 rows

Both jobs and missions live in this one array, distinguished by `isMission`
(true 9,264 / false 5,840) and linked by `parentPositionId`.

| Field | Present / 15,104 | Notes |
|---|---|---|
| `id`, `profileId`, `isMission`, `current` | 15,104 | `current` is `false` on **every** row — dead flag |
| `startDate` | 14,977 | `yyyy-MM-dd` |
| `endDate` | 12,167 | `yyyy-MM-dd` — **one bad value**, see hazards |
| `title` | 15,042 | |
| `companyName` | 14,677 | anonymized (e.g. "Quantify Partners g5bt") |
| `employerName` | 6,788 | **not** anonymized — real vendor names appear here |
| `description` | 14,262 | long free text, mixed FR/EN |
| `missionName` | 2,927 | |
| `missionContext` | 2,272 | |
| `parentPositionId` | 3,324 | → `positions[].id` |
| `aptitudeReferences` | 15,104 | avg 9.1, max 106 per position |
| `qualificationIds` | 15,104 | 586 total values |
| `customFields` | 15,104 | empty on every row |

### `positions[].aptitudeReferences[]` — 137,050 rows

`{aptitudeId, conceptId, name, type}`. No own id — the natural key is
`(position_id, aptitudeId)`. `conceptId` missing on 2,154 rows. `aptitudeId`
points at `aptitudes[].id`; note there are 137,050 references against 113,810
aptitudes, so the same aptitude is referenced from several positions.

### `completionDetails` — profile-completeness scoring

A **map keyed by rule name**, each value `{satisfied: bool, weight: int}`. 13 rules,
weights constant across all profiles and summing to 65:

`SKILLS_MIN_THREE_COMPLETE_HIGHLIGHTED` 15 · `SKILLS_MIN_FIVE_COMPLETE` 10 ·
`SKILLS_MIN_TEN_COMPLETE` 10 · `SKILLS_MIN_FIFTEEN_COMPLETE` 10 ·
`EDUCATIONS_MIN_ONE` 4 · `PROFESSIONAL_EXPERIENCES_MIN_ONE` 4 ·
`BIO_MIN_LENGTH` 3 · `LANGUAGES_MIN_ONE` 3 · `SKILLS_ALL_WITH_PROFICIENCY` 2 ·
`JOB_TITLE` 1 · `PROFILE_PICTURE` 1 · `PROFESSIONAL_EXPERIENCES_ALL_COMPLETE` 1 ·
`WORKING_LIFE_ENTRY_DATE` 1

This is a map, not a struct — Whoz can add a rule at any time and every downstream
struct schema breaks. Model it as `MAP<STRING, STRUCT<...>>` or, better, explode it
to one row per (profile, rule) as the pipeline below does.

`completionRate` is related to these weights but is **not** simply
`satisfied_weight / 65` — checked against the live export, that identity holds on
only 1,200 of the 3,390 records that have `completionDetails`. Treat the rate as an
opaque score computed by Whoz; don't try to recompute or reconcile it locally.

### `headline` — 1:1 nested object, present on 3,069 profiles

`jobTitle` is the only field with real data (7 nulls). `permissionScope` is always
SECRET. `aim`, `internationalMobility`, `mobilityDate`, `mobilityNote`,
`nationalMobility` are **null on all 3,069**. `mobilityDestinations` is empty or
null. `seekingOpportunities` is non-null on only 11 rows (all `false`).

### `skillRatings[]` — 6,792 rows, but only 83 profiles have any

`{skill: string, rating: int}`. 6,557 of 6,792 have `rating = 0`. This looks like a
deprecated field superseded by `aptitudes[].proficiency` — worth confirming with
Whoz before building anything on it. One profile alone holds 209 of these.

## Type hazards — why VARIANT-first is the right call here

These are the concrete things that would break a fixed schema or a strict cast:

1. **`completionDetails` is polymorphic.** It is a JSON object on 3,390 records and
   an **empty array `[]`** on 723. A `STRUCT` or `MAP` inference will fail or silently
   null out one of the two forms. This one alone rules out naive schema inference.

2. **`endDate = "+22015-07-31"`** on one position. An extended-year ISO date.
   `CAST(... AS DATE)` on this raises; `try_variant_get(..., 'date')` returns NULL.

3. **Mixed int/float.** `completionRate` is an int on 1,859 records and a float on
   2,254. `aptitudes[].cumulativeExperience` is an int on 32,650 and a float on
   80,704. Read both as `double`.

4. **Three timestamp precisions in the same field.** `seekingOpportunitiesLastModifiedDate`
   appears as `2025-11-26T21:25:48Z` (43 rows), `2025-07-24T13:17:15.662Z` (1,951)
   and `2026-04-02T04:30:47.310717871Z` (2 — nanoseconds). Same split on
   `createdDate` / `lastModifiedDate` / `lastExplicitUpdate` (5, 4 and 4 rows at
   second precision). Cast to `timestamp`, not to a fixed format string.

5. **Genuine key drift.** Counted over the file:
   - root objects: 6 distinct key sets; 8 of 32 keys are optional
   - `positions[]`: **26 distinct key sets**
   - `aptitudes[]`: 10 distinct key sets
   - `positions[].aptitudeReferences[]`: 2

   Absent-vs-null matters here: `hobbies` is *omitted* on 2,069 records rather than
   set to null, whereas `headline.aim` is *present and null* on all 3,069. A VARIANT
   preserves that distinction; an inferred struct erases it.

## Modelling recommendations

- **Bronze**: one row per profile, whole object in a single VARIANT. No inference,
  no rescued-data column, no failed batch when Whoz ships a new field.
- **Silver**: four child tables — `profile`, `profile_aptitude`, `profile_position`,
  `position_aptitude_ref` — plus `profile_completion_rule` from the map. Keep the
  raw VARIANT on the profile table so nothing is lost.
- Use `try_variant_get`, never `variant_get`, for anything date- or number-typed.
- Drop the always-empty arrays from your model until Whoz starts populating them;
  keep an expectation that alerts if they ever become non-empty.
- Treat `positions[].current` and `skillRatings[]` as suspect and confirm with the
  source system before anyone builds a metric on them.
- `employerName` is not anonymized in this extract — check that against whatever
  agreement covers this data before it lands in a shared catalog.

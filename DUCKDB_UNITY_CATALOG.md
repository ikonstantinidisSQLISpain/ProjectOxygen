# Querying Databricks Unity Catalog locally with DuckDB

How to read Unity Catalog tables from your own machine using DuckDB, with no Databricks
compute (no serverless, no SQL warehouse, no cluster).

Status for **this** workspace (`adb-7405610661645830`, Azure, metastore
`metastore_azure_westeurope`): **partially possible — catalog browsing works today, data reads
are blocked by three independent issues.** See [Where we actually stand](#where-we-actually-stand).

---

## 1. The short answer

**Yes, the concept is real, and yes — it uses your compute, not Databricks compute.**

Unity Catalog exposes an open REST API. An external engine (DuckDB, Spark, Trino, Snowflake,
Polars…) can:

1. Ask UC for a table's metadata and storage location.
2. Ask UC to *vend* a short-lived, narrowly-scoped cloud storage credential for that table
   (this is called **credential vending**).
3. Use that credential to read the Delta files **directly from ADLS / S3 / GCS**, and do all
   scanning, filtering and joining locally.

Databricks is only in the loop for the metadata + credential handshake — two small HTTPS calls.
No DBUs are consumed. The query itself runs entirely in your DuckDB process.

### What it costs

| Component | Cost |
|---|---|
| Databricks compute (DBUs) | **None.** No warehouse, no cluster, no serverless. |
| UC metadata + credential API calls | Free. |
| Cloud storage reads (ADLS transactions) | Billed to the storage account, normally cents. |
| Egress out of the Azure region | Billed if you pull data outside the region — **this is the one that can bite.** Full scans of large tables from a laptop in another region are not free. |
| Your CPU/RAM | Yours. Also your bottleneck. |

So "free" is roughly true for DBUs and roughly true in absolute terms for small/medium tables,
but it is **not** zero-cost for large scans — you are trading DBUs for egress plus your own
hardware.

### What you give up

- **No Photon, no cluster parallelism.** A 500 GB table that a warehouse chews through is not
  something a laptop should attempt.
- **No Databricks SQL surface.** No `MERGE`, no time-travel syntax sugar, no UDFs, no `ai_*`
  functions. DuckDB SQL only.
- **Reads only, in practice.** Write-back to managed tables from Delta clients exists but is a
  separate preview and out of scope here.
- **Row filters and column masks are not supported** — tables using them are excluded from
  credential vending entirely, by design. Don't plan to use this to work around governance.

Good fit: interactive exploration, local dev against real data, notebook/BI prototyping, dbt-duckdb
against modest tables, CI tests. Bad fit: production ETL, large aggregations, anything that
needs to run near the data.

---

## 2. Where we actually stand

Verified on this machine against this workspace on 2026-07-29.

### What works today ✅

DuckDB v1.5.3 CLI (`C:\Users\ikonstantinidis\Downloads\duckdb_cli-windows-amd64\duckdb.exe`)
already ships `unity_catalog` and `delta` as **core** extensions — no community repo needed.
Attaching the catalog and browsing metadata works right now:

```
--- ATTACH OK ---
┌────────────────────┐
│    schema_name     │
├────────────────────┤
│ bronze             │
│ default            │
│ information_schema │
│ landing            │
│ silver             │
└────────────────────┘
```

### What is blocked ❌

Three **independent** blockers stand between that and `SELECT * FROM oxygen_dev.silver.whoz_profiles`.
Fixing any one of them alone is not enough.

| # | Blocker | Scope | Who can fix |
|---|---|---|---|
| 1 | `external_access_enabled = false` on the metastore | Whole metastore | Metastore admin |
| 2 | `EXTERNAL USE SCHEMA` not granted on the schema | Per schema | Parent **catalog owner** only |
| 3 | Every table in `oxygen_dev.silver` and `oxygen_dev.bronze` is a `STREAMING_TABLE` — pipeline streaming tables and materialized views are **explicitly excluded** from credential vending | These schemas | Needs Compatibility Mode preview, or plain Delta copies |
| 4 | The DuckDB extension has **no Azure credential vending** — it assumes S3 | This workspace is Azure | Use the manual `delta_scan` path in §5 |

Blocker 4 is the nastiest and the least documented, so it gets its own section below.

Also note the exact table name: there is no `whoz_profile`. It is **`whoz_profiles`** (plural).

### Evidence for each

**1 — metastore toggle.** `databricks metastores summary` returns:

```json
{ "cloud": "azure", "external_access_enabled": false, "owner": "System user", ... }
```

With it off, the credential call fails at the last step. DuckDB got all the way to:

```
IO Error: POST Request to '.../api/2.1/unity-catalog/temporary-table-credentials' failed: 'Forbidden'
```

That `Forbidden` is the whole story — metadata fine, credentials denied.

**2 — privilege.** `databricks grants get schema oxygen_dev.silver` returns `{}`. Nothing is
granted. Note that `EXTERNAL USE SCHEMA` **must be granted explicitly**: it is not included in
`ALL PRIVILEGES`, and schema owners do **not** get it by default. Only the parent catalog owner
can grant it. This is deliberate — it's the switch that lets data leave the platform.

**3 — streaming tables.** Confirmed via the API:

```
name                          table_type
----                          ----------
whoz_profiles                 STREAMING_TABLE
whoz_profile_aptitudes        STREAMING_TABLE
whoz_profile_positions        STREAMING_TABLE
...  (all 7 in silver, plus bronze)
```

The credential-vending docs list as **not supported**: views, materialized views, *Lakeflow
pipelines streaming tables*, online tables, AI Search indexes, tables with row filters or
column masks, and OpenSharing-shared tables. Our entire medallion output is in that list.

**4 — Azure.** In `duckdb/unity_catalog`, issue [#7 "Azure support?"](https://github.com/duckdb/unity_catalog/issues/7)
is still **open** (filed June 2024), and PR [#65](https://github.com/duckdb/unity_catalog/issues/65),
which added `abfss://` detection so credentials are stored as Azure-type instead of "defaulting
to S3 behavior", was **closed unmerged** by its author. The `AWS_REGION` parameter being
*mandatory* in `CREATE SECRET (TYPE UC, ...)` even on Azure is the tell.

**Consequence:** on Azure, the `ATTACH ... (TYPE UC_CATALOG)` path gives you a working catalog
browser but will not read data. Use §5 instead.

---

## 3. One-time admin setup

Both steps are required for any of this. Neither is something to do casually on a shared
corporate metastore — step 1 is metastore-wide.

### Step 1 — enable external data access on the metastore

Metastore admin only. This is a prerequisite toggle: **on its own it grants nobody anything**,
because access still requires the per-schema privilege in step 2, which defaults to nobody.
It is also trivially reversible.

```powershell
databricks metastores update a487973c-1f78-4468-a617-03308b103d5a `
  --json '{\"external_access_enabled\": true}'
```

Verify:

```powershell
databricks metastores summary --output json | ConvertFrom-Json |
  Select-Object name, cloud, external_access_enabled
```

If this returns a permissions error, you are not a metastore admin. The metastore here is owned
by `System user`, so you'll need whoever holds account-admin rights on the SQLI Azure Databricks
account. It can also be done in the UI: **Account console → Catalog → the metastore →
External data access → Enable**.

### Step 2 — grant `EXTERNAL USE SCHEMA`

Parent catalog owner only. Grant it as narrowly as you can — per schema, to a named principal,
never to `account users`.

```sql
GRANT EXTERNAL USE SCHEMA ON SCHEMA oxygen_dev.silver TO `ikonstantinidis@sqli.com`;
```

Or via CLI:

```powershell
databricks grants update schema oxygen_dev.silver `
  --json '{\"changes\":[{\"principal\":\"ikonstantinidis@sqli.com\",\"add\":[\"EXTERNAL_USE_SCHEMA\"]}]}'
```

Verify:

```powershell
databricks grants get schema oxygen_dev.silver --output json
```

### Step 3 — deal with the streaming tables

Pick one:

**(a) Compatibility Mode** (Public Preview) — keeps the pipeline as-is. Your workspace must be
enrolled in the *External data access for pipeline datasets* preview, enabled on the schema.
It generates a read-only Delta+Iceberg v1 version of each streaming table / MV at a chosen
location. This is the "proper" answer but it is a preview and needs enrollment via your
Databricks account team.

**(b) Materialize a plain managed Delta table** — pragmatic, works today, costs one pipeline
write. Add a gold-layer table that is a normal managed table rather than a streaming table:

```sql
CREATE OR REPLACE TABLE oxygen_dev.gold.whoz_profiles_ext AS
SELECT * FROM oxygen_dev.silver.whoz_profiles;
```

Managed Delta is fully supported for read credential vending, so this is immediately readable
from DuckDB once steps 1–2 are done. Grant `EXTERNAL USE SCHEMA` on `oxygen_dev.gold` instead
of `silver`, which also keeps the external-access blast radius off your raw layers.

Recommendation: **(b)** for the PoC, **(a)** if this becomes a real access pattern.

---

## 4. Local setup

### Install

DuckDB v1.5.3+ already has what you need as core extensions. On Azure you also need `azure`:

```sql
INSTALL delta;
INSTALL azure;      -- required for abfss://
INSTALL unity_catalog;   -- alias: uc_catalog
```

Confirm:

```sql
SELECT extension_name, installed, extension_version
FROM duckdb_extensions()
WHERE extension_name IN ('unity_catalog','delta','azure');
```

### Never hardcode the token

DuckDB supports `getenv()`, so keep the PAT out of your SQL files:

```powershell
$env:DATABRICKS_TOKEN = (Select-String -Path "$env:USERPROFILE\.databrickscfg" -Pattern '^token\s*=\s*(.+)$').Matches.Groups[1].Value.Trim()
```

```sql
CREATE OR REPLACE SECRET uc_dbx (
    TYPE UC,
    TOKEN getenv('DATABRICKS_TOKEN'),
    ENDPOINT 'https://adb-7405610661645830.10.azuredatabricks.net',
    AWS_REGION 'eu-west-1'
);
```

`AWS_REGION` is mandatory even on Azure, where it is meaningless. Put anything valid there.

### Attach — and the one gotcha that costs an hour

**You must name the secret explicitly in `ATTACH`.**

```sql
-- WRONG: silently fails to resolve the endpoint
ATTACH 'oxygen_dev' AS oxy (TYPE UC_CATALOG);
-- IO Error: Could not resolve hostname error for HTTP GET to '/api/2.1/unity-catalog/schemas?...'

-- RIGHT
ATTACH 'oxygen_dev' AS oxy (TYPE UC_CATALOG, SECRET uc_dbx);
```

Without `SECRET`, the default secret lookup doesn't match (the secret's `scope` is empty), the
endpoint stays unset, and you get a bare path with no host in the error — which reads like a
network/DNS problem and is not.

### Browse

```sql
SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'oxy';
SELECT * FROM duckdb_tables() WHERE database_name = 'oxy';
```

Avoid `SHOW ALL TABLES` — see the VARIANT gotcha below.

---

## 5. The path that works on Azure: manual vending + `delta_scan`

Because the extension can't vend Azure credentials (blocker 4), do the handshake yourself and
hand DuckDB a plain Azure secret. This is more manual but it is the only route on ADLS today,
and it's the same two API calls the extension would have made.

**Prerequisite:** steps 1–3 above must be done, and the table must be a managed/external Delta
table (not a streaming table).

### 1. Get the table id

```powershell
$tid = (databricks tables get oxygen_dev.gold.whoz_profiles_ext --output json |
        ConvertFrom-Json).table_id
```

### 2. Vend a read credential

```powershell
$resp = databricks api post /api/2.1/unity-catalog/temporary-table-credentials `
  --json "{\"table_id\":\"$tid\",\"operation\":\"READ\"}" | ConvertFrom-Json
$resp | ConvertTo-Json -Depth 5
```

On Azure the response contains `url` (the `abfss://…` table root) plus one of
`azure_user_delegation_sas` (a SAS token) or `azure_aad` (a bearer token). Credentials are
short-lived — typically an hour — so this is a per-session step, not something to cache in a file.

### 3. Feed it to DuckDB

With a user-delegation SAS:

```sql
LOAD azure; LOAD delta;

CREATE OR REPLACE SECRET az_uc (
    TYPE AZURE,
    PROVIDER CONFIG,
    CONNECTION_STRING 'BlobEndpoint=https://dbxpocstor3f4h.blob.core.windows.net;SharedAccessSignature=<sas-token-without-leading-?>'
);

SELECT * FROM delta_scan('abfss://oxygen-source@dbxpocstor3f4h.dfs.core.windows.net/<path-from-url>')
LIMIT 10;
```

### 4. Wrap it

Once it works, script the whole thing so it's one command. Generate the SQL from PowerShell and
pipe it into DuckDB, so the token and SAS never touch disk:

```powershell
$sql = @"
LOAD azure; LOAD delta;
CREATE OR REPLACE SECRET az_uc (TYPE AZURE, PROVIDER CONFIG, CONNECTION_STRING '$conn');
SELECT * FROM delta_scan('$($resp.url)') LIMIT 10;
"@
$sql | & "C:\Users\ikonstantinidis\Downloads\duckdb_cli-windows-amd64\duckdb.exe"
```

Note `delta_scan` gives you a real Delta read — transaction log, correct file pruning,
deletion vectors — not a naive Parquet glob. Don't be tempted to point `read_parquet` at the
directory; you'll silently read deleted rows.

---

## 6. Gotchas

### VARIANT columns break schema enumeration

The extension has no mapping for Delta's `VARIANT` type:

```
Not implemented Error: Tried to fallback to unknown type for 'variant'
```

Worse, it appears to load metadata for **every table in a schema** when you first touch **any**
table in it. `oxygen_dev.silver` has two VARIANT columns —
`whoz_profile_aptitudes.aptitude_payload` and `whoz_profile_positions.position_payload` — and
their presence makes even `SELECT count(*) FROM oxy.silver.whoz_profiles` fail, despite
`whoz_profiles` having no VARIANT column of its own. `oxygen_dev.bronze.whoz_profiles.payload`
does the same to bronze.

Confirmed not fixed in nightly: `FORCE INSTALL unity_catalog FROM core_nightly` 404s for
v1.5.3 (`unity_catalog` isn't in the nightly repo for this version), and the nightly `delta`
build makes no difference.

**Workarounds:** cast VARIANT to `STRING` in whatever table you expose externally (do this in
the gold copy from §3b), or bypass the catalog entirely with `delta_scan` as in §5, which reads
one table and never enumerates its siblings.

### Other things that will trip you up

- **`SHOW ALL TABLES` scans every schema** in the attached catalog, so a single bad column type
  anywhere kills it. Use `duckdb_tables()` filtered to a schema.
- **PAT vs OAuth.** A PAT works. For anything long-lived prefer M2M OAuth (service principal) —
  credential vending supports it and automatic refresh, and it doesn't expire on you awkwardly.
- **Network.** Your machine needs to reach both the workspace URL *and* the storage account. IP
  access lists or Private Link on either will block this, and the error will not say so clearly.
- **Credential expiry mid-query.** Vended credentials are short-lived; long scans can die
  partway. Re-vend and retry rather than chasing the error.
- **Don't commit the PAT.** `docs/` is in git. Everything above reads it from the environment
  for that reason.

---

## 7. Alternatives worth knowing

| Approach | Local compute? | Works on Azure today? | Notes |
|---|---|---|---|
| DuckDB + `unity_catalog` ATTACH | Yes | Metadata only | Cleanest UX; blocked by extension's S3 assumption |
| DuckDB + manual vending + `delta_scan` | Yes | **Yes** | §5. Manual but functional |
| Delta Sharing (`delta-sharing` client) | Yes | Yes | Separate governance model; good for sharing out, more setup |
| Iceberg REST catalog + `iceberg` extension | Yes | Partly | Needs UniForm/Iceberg reads enabled; DuckDB IRC support for non-S3 backends is still thin |
| `databricks-sql-connector` (Python) | **No** | Yes | Uses a SQL warehouse — full SQL surface, but this is exactly the DBU cost you're avoiding |
| Export to Parquet in a volume, pull down | Yes (after export) | Yes | Crude, but genuinely free to query afterwards and dodges every blocker above |

If the goal is just "let me poke at whoz_profiles on my laptop without burning DBUs", the last
row is worth considering seriously — one pipeline task writing Parquet to a volume, and DuckDB
reads it with zero UC involvement.

---

## Sources

- [Enable external data access to Unity Catalog](https://docs.databricks.com/aws/en/external-access/admin)
- [Unity Catalog credential vending for external system access](https://learn.microsoft.com/en-us/azure/databricks/external-access/credential-vending)
- [Access Databricks data using external systems](https://docs.databricks.com/aws/en/external-access/)
- [Enable external data access to streaming tables and materialized views](https://docs.databricks.com/aws/en/external-access/external-for-pipelines)
- [Compatibility Mode](https://learn.microsoft.com/en-us/azure/databricks/external-access/compatibility-mode)
- [DuckDB Unity Catalog extension](https://duckdb.org/docs/current/core_extensions/unity_catalog)
- [duckdb/unity_catalog issue #7 — Azure support?](https://github.com/duckdb/unity_catalog/issues/7)
- [duckdb/unity_catalog PR #65 — initial Azure credential vending (closed unmerged)](https://github.com/duckdb/unity_catalog/issues/65)

# SmartIQ_PDP mapping v2 — all source columns to raw AND curated

**Change (2026-08-04, consumer request):** curated now carries **all 364**
source columns (was 357). The 7 columns the original business tab dropped —
`FileName`, `FormGuid`, `Form`, `UserEmailId`, `FirstName`, `LastName`,
`Matrix` — are restored. Raw was already all 364 and is unchanged.

| Artifact | Change |
|---|---|
| `hive_raw_curated_mapping_v2_all_columns.xlsx` | 7 rows flipped `In_Curated` N→Y with curated column/type/cast rule filled in, `Disposition` OOS→PASSTHROUGH, per-row notes; ReadMe ZONES corrected to 364; open items 1 (grain/key) and 2 (PII) updated; change-log row appended |
| `curated_ddl.sql` | all 364 data columns + `record_hash` + framework audit columns; unpartitioned |
| `smartiq-pdp-schema.conf` | contract v2.0 — trim/empty→NULL transform now on every varchar column (361 of 364; the 3 typed source columns are the exceptions) |
| `feed-smartiq-pdp.conf` | header rewritten: nothing is dropped, so alignment discards nothing; privacy note |
| `raw_ddl.sql` | unchanged (already all 364 + framework metadata) |

**Unchanged decisions:** `file_name` is the business key;
`last_modified_datetime` is the freshness column, compared as `timestamp`
via `freshness.compare_as` regardless of physical storage type.

## Privacy — needs confirmation

Carrying every column means **submitter identity (`user_email_id`,
`first_name`, `last_name`) now persists in curated**, alongside the
pre-existing preparer / marketing-rep / underwriter contact columns.
12 columns are tagged `sensitivity = "PII"` in the contract, so reject
payloads mask them — but the curated **table** holds them in the clear.
Confirm with Privacy and restrict table access accordingly.

## Guard

`SmartIqMappingConfigTest` (ingestion-core) pins this: every contract
column must exist in both DDLs, the identity columns must be PII-tagged,
the merge key and freshness column must exist in the curated target,
`record_hash` must be present, and the curated table must stay
unpartitioned. The example cannot silently drift from the framework.

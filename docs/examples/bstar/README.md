# bstar → bluestar_current — consumer notes

Two curated tables, created ahead of the pipeline so downstream work can
start against a fixed schema: `bluestar_current.priv_addr` and
`bluestar_current.sub_prem_det`. DDL in `ddl/curated_ddl.sql`.

## What the tables are

- **Latest state per business key.** One row per key, the newest version
  the source has delivered. Not history — history stays in `bstar_raw`.
- **Source is a CDC feed.** Deletes are *soft*: a deleted key stays in the
  table with `is_deleted = true` and `last_modified_op = 'D'`. Rows that
  were never deleted carry `is_deleted` **NULL**, not false — filter with
  `is_deleted IS NOT TRUE`, never `= false`.
- **Empty until first light.** The schema is final; the data arrives when
  the ingestion pipeline goes live. Build and test queries now; expect
  rows later.

## Columns

| Group | Columns | Notes |
|-------|---------|-------|
| Business | everything from the source table | `CHAR`/`VARCHAR` are `STRING` and trimmed; `SMALLINT`/`INT`/`DECIMAL` keep their source types |
| Source timestamps | `cdc_src_last_updt_ts`, `*_lcts`, `addr_eff_dt`, `addr_end_dt`, `dob` | `STRING`, as delivered. Cast in your query if you need a `TIMESTAMP` |
| Source audit | `cdc_src_actn_cd`, `last_chg_usrid` | last CDC action / last change user seen for this key |
| Delivering partition | `inc_ful_flag`, `file_date`, `file_time` | which source partition supplied the current version |
| Framework audit | `record_hash`, `create_timestamp`, `last_modified_ts`, `last_modified_op`, `last_modified_run_id`, `is_deleted` | stamped by the pipeline; `create_timestamp` is preserved across updates |

## Schema changes

Append-only. New columns are added with `ALTER TABLE … ADD COLUMNS`; existing
columns are never renamed, retyped or removed. Pin to column names, not
positions.

## Questions

Ingestion owner: see `docs/reports/HIVE_RAW_CURATED_IMPLEMENTATION_PLAN.md`.

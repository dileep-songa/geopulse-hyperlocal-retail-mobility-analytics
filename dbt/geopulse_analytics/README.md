# GeoPulse Analytics — dbt ELT Project

**dbt Project Name:** `geopulse_analytics`<br/>
**dbt Profile:** `geopulse_analytics`<br/>
**Target Platform:** Snowflake Data Cloud (`GEOPULSE_DB.ANALYTICS`)<br/>
**Source Schema:** `GEOPULSE_DB.RAW`<br/>
**Compatible Versions:** `dbt-core 1.12.7` / `dbt-snowflake 1.12.5` (Python 3.12)<br/>

---

## 1. Overview

This dbt project implements the dimensional modeling and cloud ELT transformation layer for **GeoPulse: Hyper-Local Retail Mobility Analytics**. It ingests verified spatial catchment memberships from Snowflake raw landing tables, transforms timestamps into local Indian Standard Time, isolates multi-catchment overlaps, and materializes four commercial retail intelligence marts.

---

## 2. Project Architecture & Lineage

The transformation lineage follows a modular Medallion-style pattern:

```text
GEOPULSE_DB.RAW.RAW_STORE_CATCHMENT_MEMBERSHIPS (4,051 records)
                       │
                       ▼
      models/staging/stg_store_catchment_memberships (View)
      - Standardize identifiers, types, and metric distances
      - Convert UTC -> Asia/Kolkata (IST) via CONVERT_TIMEZONE
      - Derive calendar_date, hour_of_day, and 4 diurnal dayparts
                       │
                       ▼
      models/intermediate/int_store_memberships_overlap_tagged (View)
      - Independent CTEs for ping-level & device-level multi-catchment overlap
      - Zero Cartesian fan-out (preserves exact 4,051 membership grain)
      - Flags: is_exclusive_ping, is_overlapping_ping, exclusive_device_id, multi_catchment_device_id
                       │
       ┌───────────────┼───────────────┬───────────────┐
       ▼               ▼               ▼               ▼
fct_hourly_store  fct_store_time   fct_daily_store  fct_store_overlap
   _activity         _of_day           _summary         _summary
  (Table, 81)       (Table, 16)       (Table, 8)       (Table, 4)
```

### Source Relation
- **`GEOPULSE_DB.RAW.RAW_STORE_CATCHMENT_MEMBERSHIPS`**: Landing table containing 4,051 spatial point-in-polygon join observations between synthetic GPS mobility pings and 500-metre candidate store catchment buffers. Declared in `models/staging/_sources.yml`.

### Staging & Intermediate Models (Materialized as Views)
- **`stg_store_catchment_memberships`** (`models/staging/stg_store_catchment_memberships.sql`):
  Cleans and casts raw fields, converts UTC timestamps to Indian Standard Time (`Asia/Kolkata`, UTC+05:30) as `event_timestamp_ist`, extracts local `calendar_date` and `hour_of_day`, and assigns diurnal dayparts (`morning`: 06:00–11:59, `afternoon`: 12:00–16:59, `evening`: 17:00–21:59, `night`: 22:00–05:59). Materialized as a `view`.
- **`int_store_memberships_overlap_tagged`** (`models/intermediate/int_store_memberships_overlap_tagged.sql`):
  Computes ping-level multi-store overlaps and cumulative device-level multi-catchment memberships using independent pre-aggregated CTEs. Inner-joins back to the 4,051 base rows without Cartesian multiplication. Materialized as a `view`.

### Mart Models (Materialized as Tables)
- **`fct_hourly_store_activity`** (`models/marts/fct_hourly_store_activity.sql`):
  Aggregates unique devices (`distinct_device_count`) and total pings (`ping_count`) grouped by `store_id`, `store_name`, `calendar_date`, and `hour_of_day` across 81 active store-hours. Materialized as a `table`.
- **`fct_store_time_of_day_activity`** (`models/marts/fct_store_time_of_day_activity.sql`):
  Aggregates footfall activity grouped by `store_id`, `store_name`, and `time_of_day` across 16 store-dayparts. Materialized as a `table`.
- **`fct_daily_store_summary`** (`models/marts/fct_daily_store_summary.sql`):
  Computes daily store metrics grouped by `store_id`, `store_name`, and `calendar_date` across 8 store-days, including `distinct_devices`, `ping_count`, `active_hours_count`, and visit intensity (`pings_per_device`). Materialized as a `table`.
- **`fct_store_overlap_summary`** (`models/marts/fct_store_overlap_summary.sql`):
  Produces per-store catchment exclusivity distributions across 4 candidate stores, reporting `total_pings`, `exclusive_pings`, `overlapping_pings`, `total_devices`, `exclusive_devices`, and `multi_catchment_devices`. Materialized as a `table`.

---

## 3. Configuration & Authentication

### External Profiles Setup
In compliance with repository security standards, all Snowflake connection parameters and authentication credentials must reside outside version control in the user profile directory:

- **Windows**: `%USERPROFILE%\.dbt\profiles.yml`
- **Linux / macOS**: `~/.dbt/profiles.yml`

An example configuration structure for `profiles.yml`:

```yaml
geopulse_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_snowflake_account>
      user: <your_username>
      authenticator: <externalbrowser | OAUTH_AUTHORIZATION_CODE | username_password>
      role: ACCOUNTADMIN  # or dedicated analytics role
      database: GEOPULSE_DB
      warehouse: COMPUTE_WH
      schema: ANALYTICS
      threads: 1
```

> [!WARNING]
> **Cloud Compute & Usage Notice**
> Executing dbt commands connects to your live Snowflake instance and will consume Snowflake warehouse compute credits. Never commit `profiles.yml`, tokens, passwords, or credentials to the repository.

---

## 4. Execution & Validation Commands

Execute commands from the repository root or project directory:

```powershell
# 1. Test Snowflake connection and profile validity
dbt debug --project-dir dbt/geopulse_analytics

# 2. Build models and execute complete test suite
dbt build --project-dir dbt/geopulse_analytics --target dev

# 3. Run tests only
dbt test --project-dir dbt/geopulse_analytics --target dev
```

---

## 5. Test Suite & Validation Results

The project includes **65 automated dbt tests** across source, schema, and singular integration layers:

- **10 Source Tests**: Not-null constraints and accepted value checks on raw landing columns (`movement_mode`, `data_provenance`).
- **49 Schema Tests**: Column-level null checks, primary key uniqueness, foreign-key referential integrity, and accepted value assertions on `time_of_day`.
- **6 Singular SQL Integration Tests** (`tests/`):
  - `assert_unique_ping_store_membership.sql`: Asserts uniqueness of composite primary grain `(ping_id, store_id)`.
  - `assert_fct_hourly_activity_row_count.sql`: Asserts exact 81 active store-hours.
  - `assert_fct_store_time_of_day_row_count.sql`: Asserts exact 16 store-dayparts.
  - `assert_fct_daily_store_summary_row_count.sql`: Asserts exact 8 store-days.
  - `assert_fct_store_overlap_row_count.sql`: Asserts exact 4 store rows.
  - `assert_store_overlap_reconciliation.sql`: Enforces mathematical identity ($exclusive + overlapping = total$).

**Verified Build Result**: 6 models built, 65 tests passed (0 failures, 0 errors, 0 warnings).

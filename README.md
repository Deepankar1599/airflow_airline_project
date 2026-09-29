# Airline Booking Data Pipeline — Architecture Documentation

## 1. One-line summary (lead with this in an interview)

> "I built an end-to-end, medallion-architecture data pipeline that ingests airline booking data from an API, lands it in S3, transforms it through Databricks with Unity Catalog governance, and builds tested business-layer models in dbt Cloud — the whole thing orchestrated daily by Airflow."

## 2. Architecture overview

```
   API (GitHub-hosted CSVs)
        │
        ▼
 ┌─────────────┐
 │   Airflow   │  Daily DAG, orchestrates every stage below
 └──────┬──────┘
        │ extract_load task
        ▼
 ┌─────────────┐
 │  S3 bronze  │  Raw CSVs, partitioned by date: bronze/{ds}/*.csv
 └──────┬──────┘
        │ trigger_databricks task
        ▼
 ┌───────────────────────┐
 │      Databricks        │  Reads latest bronze folder, cleans, dedupes,
 │  (PySpark + Unity      │  and UPSERTS into 3 governed silver Delta tables:
 │   Catalog, MERGE)       │  bookings, airports, passengers
 └──────┬─────────────────┘
        │ trigger_job task (dbt Cloud API)
        ▼
 ┌───────────────────────┐
 │       dbt Cloud        │  Builds and tests the gold layer on top of
 │   (gold layer models)  │  the silver Unity Catalog tables
 └──────┬─────────────────┘
        │
        ▼
   fct_bookings, daily_booking_summary
   (queryable business tables — ready for BI/reporting)
```

## 3. Why each tool is where it is

| Layer | Tool | Why |
|---|---|---|
| Orchestration | **Airflow** | Single source of truth for scheduling and task dependencies; decides *when* and *in what order* each stage runs |
| Raw storage | **AWS S3** | Cheap, durable, date-partitioned landing zone for raw files before any processing |
| Transformation & governance | **Databricks + Unity Catalog** | PySpark for heavier cleaning/joining logic; Unity Catalog provides table-level access control and a managed storage layer instead of raw file paths |
| Business modeling & testing | **dbt Cloud** | SQL-based, version-controlled, testable transformation layer close to the analytics/business logic; separates "engineering" transforms (Databricks) from "modeling" transforms (dbt) |

## 4. Data flow, stage by stage

### Stage 1 — Ingestion (Airflow `extract_load` task)
- Pulls `bookings.csv`, `airports.csv`, `passengers.csv` from a source API.
- Writes each to `s3://awsairflowproject/bronze/{ds}/` — `{ds}` is Airflow's own logical execution date, guaranteeing the S3 partition always matches the DAG run that created it.
- Runs with `retries=3` so a transient API failure doesn't fail the whole pipeline immediately.

### Stage 2 — Silver layer (Databricks, triggered via `DatabricksRunNowOperator`)
- A Databricks Job independently determines the **latest available bronze folder** (rather than trusting a passed-in date), then reads all three CSVs from it.
- Cleans, deduplicates, and casts types.
- Writes to three Unity Catalog managed Delta tables (`bookings`, `airports`, `passengers`) using a **Delta MERGE (upsert)** on each table's natural key — this makes the pipeline **idempotent**: re-running the same day never creates duplicate rows.
- `DatabricksRunNowOperator` runs synchronously, so Airflow doesn't move to the next stage until the silver tables are fully written.

### Stage 3 — Gold layer (dbt Cloud, triggered via API from a Python task)
- dbt declares the three silver tables as **sources** (not models) since they're built outside dbt.
- Builds two gold models:
  - **`fct_bookings`** — the fact table, joining bookings to airports/passengers with explicit `has_valid_airport` / `has_valid_passenger` flags (left joins, not inner — broken foreign keys are made visible rather than silently dropped).
  - **`daily_booking_summary`** — aggregated booking count, unique passengers, total/average amount, grouped by date and airport.
- Runs `dbt build`, which builds every model and runs every test in one command.

## 5. Data quality strategy

- **Generic dbt tests**: `unique`, `not_null`, `relationships` on both silver sources and gold models.
- **Singular (custom SQL) tests**, each returning 0 rows when passing:
  - No negative/zero booking amounts
  - No booking dated after its own processing timestamp
  - No orphaned bookings referencing a non-existent airport
  - No duplicate rows in the daily summary's grain (date + airport)
  - **Row-count reconciliation**: `SUM(booking_count)` in the gold summary must equal `COUNT(*)` in the fact table — catches silent row loss/duplication in aggregation logic
  - Average-amount consistency check against the underlying total/count

## 6. Key design decisions (and the trade-offs behind them)

| Decision | Reasoning | Trade-off acknowledged |
|---|---|---|
| Upsert (MERGE) instead of append/overwrite on all silver tables | Makes re-runs safe and idempotent — a core production requirement | MERGE scans the target table, so it costs more per run than a plain overwrite; fine at this scale, would reconsider for very large dimension tables |
| Left joins with explicit validity flags in `fct_bookings` | Makes broken foreign keys visible in the data instead of silently dropping rows | Slightly wider fact table than a strict inner join would produce |
| Databricks discovers the latest bronze folder itself, rather than Airflow passing the date | Keeps the Databricks job self-contained and independently testable | Doesn't correctly handle backfills for a specific past date out of the box — would pass the date explicitly for that use case |
| Single load batch (no historical backfill) | Source API has limited/rate-limited data available | Pipeline is designed for daily incremental loads; not yet proven against multiple historical days |
| dbt Cloud instead of local dbt via Airflow BashOperator | Airflow runs in a Codespace with no access to a locally-run dbt process; dbt Cloud is also a more realistic production pattern | Adds a dependency on an external managed service instead of self-hosted orchestration |
| Unity Catalog managed tables instead of raw external Delta paths | Unity Catalog blocks external writes into the metastore's managed storage root (`LOCATION_OVERLAP`); managed tables also give better governance | Slightly less direct control over exact file layout in S3 |

## 7. Problems hit and how they were resolved (strong interview material)

- **AWS Glue `AccessDeniedException`** at the account level, unrelated to IAM permissions, reproduced even on a brand-new AWS account — identified as a known AWS-side restriction rather than a config error, and pivoted the compute layer from Glue to Databricks rather than losing time on unresponsive AWS support.
- **Unity Catalog `LOCATION_OVERLAP`** error when writing external Delta files into a path that overlapped the metastore's managed storage root — resolved by switching from raw path writes (`.option("path", ...)`) to Unity Catalog managed tables (`saveAsTable`).
- **Duplicate rows from `append` mode** on static reference tables (airports/passengers) — identified that `dropDuplicates()` only dedupes within a single run's DataFrame, not against what's already in the target table, and fixed it by switching to Delta `MERGE` upserts.
- **Airflow/dbt environment mismatch** — Airflow running in a cloud Codespace had no access to a dbt project on a local machine; resolved by moving dbt to a cloud-hosted service (dbt Cloud) triggered via API instead.

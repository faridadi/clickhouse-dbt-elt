# ELT Architecture Design — Lion Parcel Assessment (Task 1)

> 📜 **Historical document — the design written before implementation.**
> Kept deliberately as a record of the design process. It is **not** updated to follow the code;
> the authoritative references for the system as built are
> [DATA_ARCHITECTURE.md](DATA_ARCHITECTURE.md) and [INFRASTRUCTURE.md](INFRASTRUCTURE.md).

## Design vs Implementation: what changed

The table below summarises every deviation between this document and the system that was
finally built, together with the reasoning. Each change was a deliberate decision that
surfaced during implementation.

| Aspect | Original design (this document) | Final implementation | Why it changed |
| --- | --- | --- | --- |
| Image versions | Postgres 15, ClickHouse `latest`, Airflow 2.9.2 | Postgres 16, ClickHouse 26.8.1, Airflow 3.3.1 | Versions are **pinned** so builds are reproducible; `latest` yields a different image over time |
| Ingestion method | The `postgresql()` function called inside every model | A `pg_catalog` database engine created once in an init script | Credentials live in exactly one place instead of being repeated — and leaked — across models |
| Data generator | A separate Python container | The `generate_retail_transactions` Airflow DAG | Saves a container, and hands lifecycle, retries and alerting to Airflow |
| dbt orchestration | `BashOperator` or Cosmos | Cosmos `DbtTaskGroup` + `LoadMode.DBT_MANIFEST` | Every model and test becomes its own Airflow task, so failures are visible per model rather than inside one opaque task |
| Model count | 3 (`stg_`, `int_`, `mart_`) | 7 (1 Bronze, 1 Silver, **5 Gold micro-marts**) | A single "summary" table cannot serve Finance, Operations and Customer Service at once without becoming ambiguous |
| Silver materialisation | `table` **or** `view` (undecided) | `view` | It contains nothing but row-by-row transformations: no extra disk, no materialisation time |
| Schedule efficiency | Not designed | `ShortCircuitOperator` (`check_new_data`) | Skips the whole pipeline during hours with no new data |
| Testing | Not designed | 24 pytest + 23 dbt data tests | Data quality and DAG integrity are enforced automatically rather than checked by hand |

---

This document contains the technical design for Task 1: building an ETL/ELT process that moves retail transaction data from a source database into a data warehouse, including correct handling of soft-delete synchronisation.

## 1. Technology Stack

Every component below runs locally inside Docker containers.

1. **Source Database**: PostgreSQL
2. **Data Warehouse**: ClickHouse
3. **Transformation Layer**: dbt (Data Build Tool) with the `dbt-clickhouse` adapter
4. **Orchestrator**: Apache Airflow
5. **Data Generator**: A Python script simulating real-time transactions

---

## 2. Source Schema (PostgreSQL)

A single primary table named `retail_transactions` in PostgreSQL.

**DDL:**

```sql
-- 1. Create the table (camelCase, matching the raw source)
CREATE TABLE retail_transactions (
    "id" VARCHAR(50) PRIMARY KEY,             -- Receipt ID
    "customerId" VARCHAR(50),
    "lastStatus" VARCHAR(50),                -- e.g. 'PROCESSING', 'SHIPPED', 'DONE'
    "posOrigin" VARCHAR(100),
    "posDestination" VARCHAR(100),
    "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- set automatically on insert
    "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- set automatically on insert/update
    "deletedAt" TIMESTAMP NULL               -- set when a transaction is soft-deleted
);

-- 2. Create the timestamp function
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW."updatedAt" = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 3. Attach the trigger to the table
CREATE TRIGGER trigger_update_timestamp
BEFORE UPDATE ON retail_transactions
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
```

**Note on soft deletes:**
Once a parcel reaches "DONE" or is cancelled, the row is **not removed** from PostgreSQL. Instead, `deleted_at` is filled with the deletion timestamp and `updated_at` is refreshed as well.

---

## 3. ELT Strategy (Ingestion & Medallion Architecture)

Moving data from PostgreSQL to ClickHouse will be handled by **dbt** using the *Medallion architecture* (Bronze, Silver, Gold). dbt will use ClickHouse's native `postgresql()` function to pull data directly, with no heavyweight ingestion tool in between.

### A. Bronze Layer (Raw Ingestion)

This table acts as a raw replica of the source data.

* **dbt model**: `stg_retail_transactions.sql`
* **Materialisation**: `incremental`
* **ClickHouse engine**: `ReplacingMergeTree(updatedAt)`
* **Ingestion logic**:
    Run `SELECT *, now() AS load_at FROM postgresql(...)` inside dbt. The added `load_at` metadata records exactly when each row was pulled into the warehouse. On incremental runs, dbt fetches only rows whose `updatedAt` in PostgreSQL is newer than the latest `updatedAt` already in ClickHouse.
* **Soft-delete handling**:
    The `ReplacingMergeTree` engine upserts automatically — it collapses rows sharing an `id` and keeps only the version with the newest `updatedAt`. A row soft-deleted in PostgreSQL therefore arrives in ClickHouse with `deletedAt` populated, **without being physically removed**. History stays intact.

### B. Silver Layer (Cleansed / Conformed)

This table holds cleaned, ready-to-use data.

* **dbt model**: `int_retail_transactions_cleansed.sql`
* **Materialisation**: `table` or `view`
* **Transformations**:
  * **Column normalisation**: rename the source's camelCase columns (`customerId`, `updatedAt`, and so on) to standard snake_case (`customer_id`, `updated_at`), matching the target shown in the brief.
  * Text standardisation, for example forcing `lastStatus` to UPPERCASE.
  * A boolean `is_deleted` flag, `true` when `deletedAt IS NOT NULL`, so analysts can distinguish active from inactive transactions at a glance.

### C. Gold Layer (Aggregated)

This table holds aggregates for business intelligence.

* **dbt model**: `mart_daily_transaction_summary.sql`
* **Materialisation**: `table`
* **Transformations**:
  * Count daily transactions, grouped by `last_status` or by the `pos_origin` → `pos_destination` route.

---

## 4. Orchestration with Apache Airflow

* Airflow acts as the scheduler.
* Per the brief, the ETL/ELT process must run **hourly**.
* The Airflow DAG will be configured with `schedule_interval` set to `@hourly`.
* Airflow will use a `BashOperator` (or Cosmos) to enter the dbt project directory and run `dbt run --select stg_retail_transactions+`.

---

## 5. Data Simulation (Dummy Data Generator)

A separate Python container will run in the background to simulate a live transaction system:

1. Randomly `INSERT` hundreds of new transactions per minute.
2. Randomly `UPDATE` unfinished transactions, advancing their status.
3. Randomly set `deletedAt` (soft delete) on transactions already marked 'DONE'.

---

## 6. Docker Compose Configuration & Default Credentials

To keep the assessment light on the reviewer's laptop, `compose.yml` is designed to be resource-friendly, with only three core services.

### A. PostgreSQL (Source Database)

* **Image**: `postgres:15-alpine` (Alpine for a minimal footprint)
* **Port**: `5432`
* **Credentials**:
  * Database: `lion_source`
  * User: `lion_user`
  * Password: `lion_password`

### B. ClickHouse (Data Warehouse)

* **Image**: `clickhouse/clickhouse-server:latest`
* **Ports**: `8123` (HTTP API), `9000` (native client)
* **Credentials**:
  * Database: `lion_dwh`
  * User: `lion_user`
  * Password: `lion_password`

### C. Apache Airflow (Standalone)

Rather than a `CeleryExecutor` — which requires several containers (Redis, separate workers, a separate Postgres for metadata) — Airflow is condensed into **a single service** via `airflow standalone`. That command starts the scheduler and webserver together and uses SQLite for Airflow's internal metadata, all in one container. Well suited to testing and assessment.

* **Image**: `apache/airflow:3.3.1` (Python 3)
* **Port**: `8080` (Airflow web UI)
* **UI login (Simple Auth)**:
  * Username: `admin`
  * Password: `admin`

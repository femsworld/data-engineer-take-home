# Architecture Design: Audicin Data Lakehouse

## 1. Architectural Overview
For this project, I implemented a **Medallion Architecture** (Bronze, Silver, Gold), to create a robust, auditable data pipeline. This structure was chosen to separate the concerns of raw data preservation, data quality enforcement, and business logic application. using **Python** and **DuckDB**.

Bronze: Raw, immutable ingestion.

Silver: Cleaned, deduplicated, and enriched "Source of Truth."

Gold: High-level aggregated KPIs for business stakeholders.

## 2. Technical Choices & Reasoning
**Storage Format & Tooling**
**DuckDB**: Chosen as the core engine for its vectorized OLAP performance. It provides "Warehouse-grade" SQL capabilities (similar to Snowflake/BigQuery) but runs locally, offering extreme speed without infra overhead.

**Storage Choice**: I used DuckDB’s native columnar format for internal processing. In a production cloud environment, this would transition to Parquet/Iceberg on S3 to allow for schema evolution and cost-effective long-term storage.

**Partitioning & Clustering Strategy**
In this implementation, Clustering is achieved by sorting the Silver and Gold tables during creation (e.g., ORDER BY event_ts).

**Why**: In columnar storage, sorting by time (clustering) allows the engine to perform "Row-Group Skipping." When querying specific dates, DuckDB physically skips irrelevant data blocks, significantly reducing I/O.

**Idempotency & Incremental Strategy**
**Idempotency**: The pipeline is fully idempotent through the use of CREATE OR REPLACE TABLE statements. Re-running the pipeline (or a partial failure) will not result in duplicated data; it will safely overwrite the existing state.

**Incremental Potential**: While this project uses a full-refresh logic for simplicity, the Silver layer is structured using event_id and timestamp. In a production setting, this would transition to a MERGE (UPSERT) operation based on the event_id to handle incremental updates.

### 3. Data Quality & Handling the "Traps"
**Corrupted Rows (Quarantine Strategy)**
**Detection**: I used a "Schema-on-Read" strategy in Bronze, forcing all messy fields (like amount) to VARCHAR.

**Quarantine**: Data that fails type validation (e.g., the "ten" string trap) is diverted into quarantine_ tables using TRY_CAST.

**Why**: This ensures the pipeline never crashes. Bad data is "segregated" for manual audit rather than deleted, maintaining 100% data lineage.

**Duplicates & Conflicting Events**
**Trap**: The dataset contained repeated event_ids with different timestamps or values.

**Solution**: I utilized the QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts DESC) = 1 pattern. This ensures a "Latest-Version-Wins" strategy, which is the industry standard for handling out-of-order event streams.

**Schema Evolution & Timestamp Normalization**
**Evolution**: By ingesting into Bronze as VARCHAR, the system is immune to schema changes (new columns) or type shifts at the source.

Normalization: I used a COALESCE of multiple strptime patterns to standardize inconsistent ISO formats and UTC offsets into a single, unified TIMESTAMP type in Silver.

### 4. Business Logic (Gold Layer)
**Daily Active Users (DAU)**
**Bot Filtering**: A critical "Senior" choice was implemented here. I identified users with non-human activity bursts (>20 events per second). These are flagged as is_bot in Silver and strictly excluded from Gold DAU and Revenue metrics to prevent inflated business KPIs.

**Revenue & MRR**
**Daily Revenue (Net/Gross):** Gross revenue tracks all purchases. Net revenue subtracts refunds by applying a negative multiplier to refund event types.

**MRR (Monthly)**: I chose a Monthly grain for MRR. Unlike daily revenue, MRR is a financial stability metric; monthly aggregation smooths out billing cycles and provides a clearer growth trend for stakeholders.

**Backfill Strategy**
To rebuild historical data correctly, the pipeline is designed to be re-runnable. By clearing the audicin_lakehouse.db and running process.py, the system performs a full backfill from the raw files, reapplying the deduplication and bot-filtering logic to ensure historical accuracy.

**How to Run**
````bash
Install dependencies: pip install -r requirements.txt

Run the pipeline: python src/process.py

Run the test suite: python -m pytest tests/

Query the results: python src/query.py
````

# Audicin Data Engineering Take-Home Task

## 1. Overview
This repository implements a high-performance Medallion Lakehouse architecture (Bronze, Silver, Gold) designed to process Audicin's wearable, subscription, and marketing data.

The pipeline is built to be idempotent, fault-tolerant, and capable of detecting complex data "traps" like non-numeric amount fields ("ten"), duplicate event IDs, and non-human bot activity.

**Tech Stack:** - **Language:** Python 3.10+
- **Language:** DuckDB (OLAP-optimized, local storage)
- **Testing:** Pytest
- **Libraries:** Pandas (for initial ingestion & data cleaning)

---

## 2. Project Structure

```text
data-engineer-take-home/
├── data/               # CSV, JSON, NDJSON files
├── src/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   └── process.py
│   └── query.py        # SQL Utility to inspect results
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
├── DESIGN.md           # Documentation of architectural decisions
├── requirements.txt
└── README.md
└── audicin_lakehouse.db # Generated DuckDB database
```

### 3. Setup & Execution
Prerequisites
EIt is recommended to use a virtual environment. Install dependencies using:

```bash
pip install -r requirements.txt
```

- Decision Note: I designed the modules to be runnable individually for debugging (e.g., python src/gold.py), but process.py ensures the correct dependency order.


### 4. Running the Pipeline
This script will output the top rows of the Gold tables directly to your terminal:
```bash
python src/process.py
```
Note: This will generate audicin_lakehouse.db in your root directory.

**Running Diagnostics**
To verify the results and audit the quarantined records:

```bash
python src/query.py
python src/querytable.py
```

**Running the Test Suite**
The project includes a robust testing suite that uses in-memory databases to verify logic:

```bash
python -m pytest
```

### 4. Key Business Outputs (Gold Layer)
TThe pipeline produces the following requirement-compliant tables, clustered by date for query performance:

Table Name,Business Question Answered
daily_active_users,DAU count (filtered for human-only activity).
daily_revenue_gross,Total revenue before refunds.
daily_revenue_net,Refund-adjusted daily revenue.
mrr_monthly,Monthly Recurring Revenue from active subscriptions.
weekly_cohort_retention,User stickiness based on signup week.
cac_by_channel,Customer Acquisition Cost per marketing channel.
ltv_per_user,Lifetime value per user (net of refunds).
ltv_cac_ratio,Efficiency ratio of LTV vs. CAC.


### 5. Decision Notes & Handling & Robustness
- `Bot Detection: I implemented behavioral analysis in the Silver layer to flag users with activity bursts (>20 events/sec). These are flagged as is_bot and excluded from Gold analytics.

- `The 'ten' Trap`: Handled via a Quarantine Strategy. Using TRY_CAST, non-numeric amount strings are diverted to quarantine_events for audit rather than crashing the pipeline.

- `Duplicates & Out-of-Order Events`: Handled via QUALIFY ROW_NUMBER() in Silver. This implements a "Latest-Version-Wins" strategy based on event timestamps.

- `Negative Marketing Spend`: Automatically identified and moved to quarantine_marketing.

- `Timestamp Inconsistency`: Normalizes multiple ISO and string formats into a unified UTC TIMESTAMP type.


Otherwise, use the provided src/query.py to run custom SQL queries.

### 6. Architectural Reasoning
For a deep dive into the decisions regarding partitioning, idempotency, and storage formats, Please see DESIGN.md.

### 7. How to Query via SQL
If you have the DuckDB CLI installed, you can query the database directly:

```bash
duckdb audicin_lakehouse.db "SELECT * FROM daily_revenue_net LIMIT 10;"
```

## 8. Future Improvements & Scalability
While this implementation is robust for the provided dataset, the following enhancements would be prioritized for a production-scale deployment:

* **Migration to Apache Spark:** As data volume grows beyond the limits of a single vertical node, migrating the processing engine to Spark would allow for horizontal scaling.
    * **Large-Scale Joins:** Spark’s shuffle mechanics are superior for calculating LTV across billions of historical records.
    * **Lakehouse Standards:** Native support for **Delta Lake** or **Apache Iceberg** would provide ACID transactions and "Time Travel" capabilities.
    * **Streaming:** Spark Structured Streaming could process the NDJSON events in real-time micro-batches for live DAU tracking.
* **Dbt Integration:** Transitioning SQL logic to dbt to leverage built-in lineage documentation and automated data quality testing (e.g., `not_null`, `unique`).
* **Enhanced Data Quality (Circuit Breakers):** Integrating a framework like **Great Expectations** to stop the pipeline if specific thresholds are met (e.g., if the refund-to-purchase ratio exceeds a set anomaly percentage).
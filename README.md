# Data Engineering Take‑Home (Lakehouse Mini Project)

This repository contains a synthetic dataset and a set of requirements to assess the following **data engineering** skills:
data modeling, incremental processing, idempotency, messy-data handling, and clear architectural reasoning.

## What you get

### Dataset (in `./data`)
- `events.ndjson` — raw application events (NDJSON). **Messy on purpose**.
- `subscriptions.json` — subscription records (a JSON array; not NDJSON).
- `marketing_spend.csv` — daily marketing spend by channel.

### Key intentional “traps” (read this carefully)
The data includes issues you should handle explicitly in your design:
- **Duplicate events** (same `event_id` repeated)
- **Conflicting duplicates** (same `event_id`, different payload such as `amount`/`currency`)
- **Out‑of‑order events** (file order is not time order)
- **Late/early timestamps** (including refunds that appear earlier than purchase timestamps)
- **Schema evolution** (`schema_version` 1 and 2)
- **Inconsistent timestamp formats** (ISO Z, ISO with offset, and some `YYYY-MM-DD HH:MM:SS`)
- **Corrupted rows** (invalid JSON lines in the NDJSON)
- **Missing fields / nulls** (e.g., `user_id` missing/null)
- **Marketing spend gaps** (missing days) + **duplicate rows** + **negative spend** row
- **Subscriptions edge cases**: overlaps, reactivations, and a **duplicate `subscription_id`**

Your solution should be robust to these issues and explain the chosen behavior.

## Business questions

Build “gold” analytics tables that can answer:

1. **DAU**: daily active users
2. **Revenue**: gross and net (refund‑adjusted) daily revenue
3. **MRR**: monthly recurring revenue (subscription‑based; *not* the same as revenue)
4. **Weekly cohort retention**: based on signup week
5. **CAC**: customer acquisition cost (paid conversions / spend)
6. **LTV**: per user lifetime value (refund‑adjusted)
7. **LTV:CAC** ratio

## Expected deliverables

### 1) Architecture & reasoning (required)
Provide a short document (in your README or separate `DESIGN.md`) describing:
- Bronze/Silver/Gold layers (or equivalent) and why
- Storage format choices (e.g., Parquet/Iceberg/Delta; or DuckDB/SQLite; or warehouse tables)
- Partitioning/clustering strategy
- Incremental strategy (how you avoid full refreshes)
- Idempotency strategy (how re‑runs and partial failures behave)
- How you handle schema evolution + timestamp normalization
- How you handle corrupted rows (quarantine strategy)
- Backfill strategy (how you rebuild historical data correctly)

### 2) Implementation (required)
Implement a runnable pipeline that produces gold tables/views:
- `daily_active_users`
- `daily_revenue_gross`
- `daily_revenue_net`
- `mrr_daily` (or `mrr_monthly` — explain your choice)
- `weekly_cohort_retention`
- `cac_by_channel` (or overall CAC — explain)
- `ltv_per_user`
- `ltv_cac_ratio`

You may use any stack you prefer (examples):
- Python + DuckDB + Parquet
- Spark + Iceberg/Delta/Hudi
- dbt + BigQuery/Snowflake/Postgres
- SQL + local warehouse

> **Timebox**: ~4–6 hours. Prioritize correctness + clarity over “production completeness”.

### 3) How to run (required)
Include clear instructions in the README:
- Setup steps
- Commands to run the pipeline
- Where outputs are written
- How to run tests (if provided)

### 4) Tests + observability (strong bonus)
- Data quality checks (uniqueness, not‑null, accepted values, freshness)
- Unit tests for critical transforms
- Simple metrics/alerts strategy (volume anomaly, duplicate rate, late event rate)

## Dataset reference

### `events.ndjson`
Each line is an event object **or** an intentionally corrupted line.

Common fields:
- `event_id` (string) — supposed to be unique but is not always
- `user_id` (string | null) — may be missing/null in some rows
- `event_type` (string) — one of: `signup`, `login`, `page_view`, `purchase`, `refund`, `trial_start`, `trial_convert`, `cancel`
- `timestamp` (string) — inconsistent formats on purpose
- `schema_version` (int) — 1 or 2

Purchase/refund fields:
- `amount` (number) — may be negative in a tiny number of rows
- `currency` (string) — only in schema v2 (but can be present elsewhere due to corruption)
- `tax` (number) — optional in v2
- `refers_to_event_id` (string) — present on refunds (links to purchase event_id)

Signup fields:
- `acquisition_channel` (string) — e.g., Google/Facebook/Organic/etc.

### `subscriptions.json`
A JSON array of subscription records.

Fields:
- `subscription_id` (string) — **not unique** in this dataset (intentional trap)
- `user_id` (string)
- `plan_id` (string) — `basic`, `pro`, `team`
- `price` (number)
- `currency` (string)
- `start_date` (YYYY-MM-DD)
- `end_date` (YYYY-MM-DD | null)
- `status` (`active` | `canceled`)
- `created_at` (timestamp)

Edge cases:
- Overlapping subscriptions for a user
- Reactivations after churn
- Duplicate subscription id with differing status

### `marketing_spend.csv`
Columns:
- `date` (YYYY-MM-DD)
- `channel` (string)
- `spend` (number)

Edge cases:
- Missing dates for some channels
- Duplicate rows
- One negative spend row (intentional)

## Submission
- Provide your code (repo)
- Include `README` instructions
- Include outputs (or a way to generate them) and any notes on decisions

---

### Notes
This dataset was generated with a fixed random seed for reproducibility. The exact counts will vary slightly depending on how you parse corrupted rows, but the **issues are intentional** and should be addressed.

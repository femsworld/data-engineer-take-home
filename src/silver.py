import duckdb

def run_silver(con):
    """
    Cleans Bronze data and segregates invalid records into quarantine tables.
    
    Validation Logic:
    - Marketing: Spend must be numeric and >= 0.
    - Events: Amount must be numeric, and critical IDs must be present.
    - Result: 'silver_x' tables contain clean data; 'quarantine_x' tables contain rejected data for auditing.
    """
    print("--- Starting Silver Layer: Cleaning & Quarantine ---")
    
    # 1. CLEAN MARKETING vs QUARANTINE
    # Move valid spend to silver
    con.execute("""
        CREATE OR REPLACE TABLE silver_marketing AS 
        SELECT date::DATE as date, channel, try_cast(spend as DOUBLE) as spend
        FROM bronze_marketing 
        WHERE try_cast(spend as DOUBLE) IS NOT NULL AND try_cast(spend as DOUBLE) >= 0
    """)
    
    # Audit negative or non-numeric spend
    con.execute("""
        CREATE OR REPLACE TABLE quarantine_marketing AS 
        SELECT *, 
               CASE WHEN try_cast(spend as DOUBLE) IS NULL THEN 'Non-numeric spend'
                    WHEN try_cast(spend as DOUBLE) < 0 THEN 'Negative spend'
               END as rejection_reason
        FROM bronze_marketing 
        WHERE try_cast(spend as DOUBLE) IS NULL OR try_cast(spend as DOUBLE) < 0
    """)

    # 2. CLEAN EVENTS vs QUARANTINE
    con.execute("""
        CREATE OR REPLACE TABLE silver_events AS
        WITH validated AS (
            SELECT *,
                COALESCE(
                    try_cast(timestamp as TIMESTAMP),
                    try_cast(strptime(timestamp, '%Y-%m-%d %H:%M:%S') as TIMESTAMP),
                    try_cast(strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ') as TIMESTAMP)
                ) as event_ts,
                try_cast(amount as DOUBLE) as amount_num
            FROM bronze_events 
            WHERE user_id IS NOT NULL 
              AND (try_cast(amount as DOUBLE) IS NOT NULL OR amount IS NULL)
        )
        SELECT 
            event_id, user_id, event_type, event_ts, 
            COALESCE(amount_num, 0) as amount, -- Only 0 if it was truly NULL, not if it was 'ten'
            currency, refers_to_event_id
        FROM validated
        QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts DESC) = 1
    """)

    # Audit 'ten' trap and missing user_id
    con.execute("""
        CREATE OR REPLACE TABLE quarantine_events AS 
        SELECT *,
               CASE WHEN user_id IS NULL THEN 'Missing user_id'
                    WHEN try_cast(amount as DOUBLE) IS NULL AND amount IS NOT NULL THEN 'Invalid numeric amount (e.g. ten)'
               END as rejection_reason
        FROM bronze_events 
        WHERE user_id IS NULL 
           OR (try_cast(amount as DOUBLE) IS NULL AND amount IS NOT NULL)
    """)

    # 3. Clean Subscriptions (Deduplicate + Quarantine)
    # First, Quarantine anything truly broken (missing ID or price)
    con.execute("""
        CREATE OR REPLACE TABLE quarantine_subscriptions AS
        SELECT *, 'Missing critical ID or price' as rejection_reason
        FROM bronze_subscriptions
        WHERE subscription_id IS NULL OR price IS NULL
    """)

    # Second, Create the clean "Current State" table for Gold MRR
    con.execute("""
        CREATE OR REPLACE TABLE silver_subscriptions AS
        SELECT * FROM bronze_subscriptions
        WHERE subscription_id IS NOT NULL AND price IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY created_at DESC) = 1
    """)

    # Metrics for logs
    q_ev = con.execute("SELECT count(*) FROM quarantine_events").fetchone()[0]
    q_mk = con.execute("SELECT count(*) FROM quarantine_marketing").fetchone()[0]
    q_sub = con.execute("SELECT count(*) FROM quarantine_subscriptions").fetchone()[0]
    
    print(f"Silver complete. Quarantined: {q_ev} events, {q_mk} marketing rows, {q_sub} subscriptions.")
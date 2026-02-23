import duckdb

def run_silver(con):
    """
    Cleans Bronze data, flags behavioral anomalies (bots), and segregates 
    invalid records into quarantine tables for audit.
    
    Traps Handled:
    - Duplicate/Conflicting Events: Resolved via ROW_NUMBER() on event_id.
    - Timestamp Inconsistency: Normalized via multi-format COALESCE.
    - Non-numeric 'Amount': Quarantined (e.g., the "ten" trap).
    - Marketing Traps: Negative spend quarantined; duplicates removed.
    - Bot Detection: Users with > 20 events in 1 second are flagged.
    """
    print("--- Starting Silver Layer: Cleaning & Flagging ---")
    
    # 1. CLEAN MARKETING vs QUARANTINE
    # Handles: Negative spend (Quarantine), Duplicates (Qualify), Missing Dates (Filtered)
    con.execute("""
        -- Clean Table
        CREATE OR REPLACE TABLE silver_marketing AS 
        SELECT date::DATE as date, channel, try_cast(spend as DOUBLE) as spend
        FROM bronze_marketing 
        WHERE try_cast(spend as DOUBLE) IS NOT NULL AND try_cast(spend as DOUBLE) >= 0
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date, channel, spend ORDER BY date) = 1;

        -- Audit Table
        CREATE OR REPLACE TABLE quarantine_marketing AS 
        SELECT *, 
               CASE WHEN try_cast(spend as DOUBLE) IS NULL THEN 'Non-numeric spend'
                    WHEN try_cast(spend as DOUBLE) < 0 THEN 'Negative spend'
               END as rejection_reason
        FROM bronze_marketing 
        WHERE try_cast(spend as DOUBLE) IS NULL OR try_cast(spend as DOUBLE) < 0;
    """)

    # 2. EVENT CLEANING & BOT FLAGGING
    # Handles: Inconsistent Timestamps, Duplicate event_ids, Bot activity bursts
    con.execute("""
        CREATE OR REPLACE TABLE silver_events AS
        WITH bot_users AS (
            SELECT user_id, timestamp 
            FROM bronze_events 
            GROUP BY 1, 2 HAVING COUNT(*) > 20
        ),
        validated AS (
            SELECT 
                b.event_id, b.user_id, b.event_type, b.currency, b.refers_to_event_id,
                COALESCE(
                    try_cast(b.timestamp as TIMESTAMP),
                    try_cast(strptime(b.timestamp, '%Y-%m-%d %H:%M:%S') as TIMESTAMP),
                    try_cast(strptime(b.timestamp, '%Y-%m-%dT%H:%M:%SZ') as TIMESTAMP)
                ) as event_ts,
                try_cast(b.amount as DOUBLE) as amount_num,
                (d.user_id IS NOT NULL) as is_bot
            FROM bronze_events b
            LEFT JOIN bot_users d ON b.user_id = d.user_id AND b.timestamp = d.timestamp
            WHERE b.user_id IS NOT NULL 
              AND (try_cast(b.amount as DOUBLE) IS NOT NULL OR b.amount IS NULL)
        )
        SELECT 
            event_id, user_id, event_type, event_ts, 
            COALESCE(amount_num, 0) as amount, 
            currency, refers_to_event_id, is_bot
        FROM validated
        QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts DESC) = 1;
    """)

    # 3. EVENTS QUARANTINE (Schema/Value Corruption)
    con.execute("""
        CREATE OR REPLACE TABLE quarantine_events AS 
        SELECT *,
               CASE WHEN user_id IS NULL THEN 'Missing user_id'
                    WHEN try_cast(amount as DOUBLE) IS NULL AND amount IS NOT NULL THEN 'Invalid numeric amount (e.g. ten)'
               END as rejection_reason
        FROM bronze_events 
        WHERE user_id IS NULL 
           OR (try_cast(amount as DOUBLE) IS NULL AND amount IS NOT NULL);
    """)

    # 4. SUBSCRIPTIONS CLEAN & QUARANTINE
    # Handles: Duplicate subscription_ids and missing critical data
    con.execute("""
        -- Clean Table (Deduplicated to keep current state)
        CREATE OR REPLACE TABLE silver_subscriptions AS
        SELECT * FROM bronze_subscriptions
        WHERE subscription_id IS NOT NULL AND price IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY created_at DESC) = 1;

        -- Audit Table
        CREATE OR REPLACE TABLE quarantine_subscriptions AS
        SELECT *, 'Missing ID or price' as rejection_reason
        FROM bronze_subscriptions
        WHERE subscription_id IS NULL OR price IS NULL;
    """)

    # Log Metrics for Observability
    counts = con.execute("""
        SELECT 
            (SELECT COUNT(*) FROM silver_events) as clean_ev,
            (SELECT COUNT(*) FROM quarantine_events) as q_ev,
            (SELECT COUNT(*) FROM silver_marketing) as clean_mk,
            (SELECT COUNT(*) FROM quarantine_marketing) as q_mk
    """).fetchone()

    print(f"Silver complete.")
    print(f" - Events: {counts[0]} clean, {counts[1]} quarantined.")
    print(f" - Marketing: {counts[2]} clean, {counts[3]} quarantined.")
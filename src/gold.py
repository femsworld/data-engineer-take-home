import duckdb

def run_gold(con):
    """
    Produces requirement-compliant Gold tables, filtering out flagged bots.
    Data is sorted during creation to ensure optimal clustering for columnar storage.
    """
    print("--- Starting Gold Layer: Analytics ---")
    
    # Reusable snippet for human-only events
    human_events = "SELECT * FROM silver_events WHERE is_bot = FALSE"

    # 1. daily_active_users
    con.execute(f"""
        CREATE OR REPLACE TABLE daily_active_users AS 
        SELECT event_ts::DATE as date, COUNT(DISTINCT user_id) as dau 
        FROM ({human_events}) 
        GROUP BY 1
        ORDER BY date
    """)

    # 2. daily_revenue_gross
    con.execute(f"""
        CREATE OR REPLACE TABLE daily_revenue_gross AS 
        SELECT event_ts::DATE as date, SUM(amount) as gross_revenue 
        FROM ({human_events}) 
        WHERE event_type = 'purchase' 
        GROUP BY 1
        ORDER BY date
    """)

    # 3. daily_revenue_net
    con.execute(f"""
        CREATE OR REPLACE TABLE daily_revenue_net AS 
        SELECT event_ts::DATE as date, 
               SUM(CASE WHEN event_type = 'purchase' THEN amount 
                        WHEN event_type = 'refund' THEN -amount ELSE 0 END) as net_revenue 
        FROM ({human_events}) 
        GROUP BY 1
        ORDER BY date
    """)

    # 4. mrr_monthly
    con.execute("""
        CREATE OR REPLACE TABLE mrr_monthly AS 
        SELECT date_trunc('month', created_at)::DATE as month, SUM(price) as mrr 
        FROM silver_subscriptions 
        WHERE status = 'active' 
        GROUP BY 1
        ORDER BY month
    """)

    # 5. weekly_cohort_retention
    con.execute(f"""
        CREATE OR REPLACE TABLE weekly_cohort_retention AS
        WITH user_signup AS (
            SELECT user_id, date_trunc('week', MIN(event_ts)) as signup_week
            FROM ({human_events}) WHERE event_type = 'signup' GROUP BY 1
        ),
        user_activity AS (
            SELECT DISTINCT user_id, date_trunc('week', event_ts) as activity_week
            FROM ({human_events})
        )
        SELECT 
            s.signup_week, 
            ((a.activity_week - s.signup_week)/7)::INT as week_number, 
            COUNT(DISTINCT a.user_id) as active_users
        FROM user_signup s 
        JOIN user_activity a ON s.user_id = a.user_id 
        GROUP BY 1, 2
        ORDER BY signup_week, week_number
    """)

    # 6. cac_by_channel
    con.execute(f"""
        CREATE OR REPLACE TABLE cac_by_channel AS 
        SELECT m.channel, SUM(m.spend) / NULLIF(COUNT(DISTINCT e.user_id), 0) as cac 
        FROM silver_marketing m 
        LEFT JOIN ({human_events}) e ON m.date = e.event_ts::DATE AND e.event_type = 'signup' 
        GROUP BY 1
        ORDER BY cac DESC
    """)

    # 7. ltv_per_user
    con.execute(f"""
        CREATE OR REPLACE TABLE ltv_per_user AS 
        SELECT user_id, 
               SUM(CASE WHEN event_type = 'purchase' THEN amount 
                        WHEN event_type = 'refund' THEN -amount ELSE 0 END) as user_ltv 
        FROM ({human_events}) 
        GROUP BY 1
        ORDER BY user_ltv DESC
    """)

    # 8. ltv_cac_ratio
    con.execute("""
        CREATE OR REPLACE TABLE ltv_cac_ratio AS 
        SELECT 
            AVG(user_ltv) as avg_ltv, 
            (SELECT AVG(cac) FROM cac_by_channel) as avg_cac, 
            AVG(user_ltv) / NULLIF((SELECT AVG(cac) FROM cac_by_channel), 0) as ltv_cac_ratio 
        FROM ltv_per_user
    """)
    
    print("Gold tables created successfully with clustering (sorting).")
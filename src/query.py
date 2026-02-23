import duckdb
import pandas as pd
import os

# Get absolute path to ensure we hit the generated DB
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'audicin_lakehouse.db')

def run_diagnostics():
    """
    Utility script to verify the health of the Lakehouse.
    Checks Gold analytics, the Bot Flagging results, and Quarantine audits.
    """
    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        return

    conn = duckdb.connect(db_path)

    def print_section(title, query):
        print(f"\n{'='*15} {title} {'='*15}")
        try:
            df = conn.execute(query).df()
            if df.empty:
                print("No records found.")
            else:
                print(df.to_string(index=False))
        except Exception as e:
            print(f"Error querying {title}: {e}")

    # 1. BOT DETECTION AUDIT
    # Proves that the bot-flagging logic in Silver actually worked
    print_section("SILVER: BOT DETECTION SUMMARY", 
                  "SELECT is_bot, COUNT(*) as event_count, COUNT(DISTINCT user_id) as user_count FROM silver_events GROUP BY 1")

    # 2. GOLD ANALYTICS CHECKS (Requirement compliance)
    print_section("GOLD: DAILY NET REVENUE (HUMANS ONLY)", 
                  "SELECT * FROM daily_revenue_net ORDER BY date DESC LIMIT 5")

    print_section("GOLD: MRR MONTHLY", 
                  "SELECT * FROM mrr_monthly ORDER BY month DESC")

    print_section("GOLD: LTV & CAC RATIO", 
                  "SELECT ROUND(avg_ltv, 2) as LTV, ROUND(avg_cac, 2) as CAC, ROUND(ltv_cac_ratio, 4) as Ratio FROM ltv_cac_ratio")

    # 3. DATA QUALITY AUDIT (QUARANTINE INSPECTION)
    # Proves the 'ten' trap and negative spend were caught
    print_section("QUARANTINE: REJECTED EVENTS SAMPLE", 
                  "SELECT event_id, event_type, amount, rejection_reason FROM quarantine_events LIMIT 3")

    print_section("QUARANTINE: REJECTED MARKETING", 
                  "SELECT date, channel, spend, rejection_reason FROM quarantine_marketing")

    conn.close()

if __name__ == "__main__":
    run_diagnostics()
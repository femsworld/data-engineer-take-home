import pytest
import duckdb
from src.gold import run_gold

@pytest.fixture
def silver_data():
    con = duckdb.connect(':memory:')
    con.execute("""
        CREATE TABLE silver_events (event_id VARCHAR, user_id VARCHAR, event_type VARCHAR, event_ts TIMESTAMP, amount DOUBLE, is_bot BOOLEAN);
        INSERT INTO silver_events VALUES 
        ('e1', 'u1', 'signup', '2026-01-01', 0, false),   -- Signup on Jan 1st
        ('e2', 'u1', 'purchase', '2026-01-02', 100.0, false),
        ('e3', 'u1', 'refund', '2026-01-03', 20.0, false); 
        
        CREATE TABLE silver_subscriptions (subscription_id VARCHAR, price DOUBLE, created_at TIMESTAMP, status VARCHAR);
        INSERT INTO silver_subscriptions VALUES ('s1', 50.0, '2026-01-01', 'active');

        CREATE TABLE silver_marketing (date DATE, channel VARCHAR, spend DOUBLE);
        INSERT INTO silver_marketing VALUES 
        ('2026-01-01', 'Search', 10.0),    -- This will match the signup on Jan 1st
        ('2026-01-05', 'Offline', 500.0); -- MOVED TO JAN 5th (No signups on this day)
    """)
    return con

def test_gold_net_revenue(silver_data):
    run_gold(silver_data)
    # 100 purchase - 20 refund = 80
    net_rev = silver_data.execute("SELECT net_revenue FROM daily_revenue_net WHERE date = '2026-01-03'").fetchone()[0]
    assert net_rev == -20.0 # On that specific day, it was a net loss of 20

def test_gold_cac_division_by_zero_safety(silver_data):
    run_gold(silver_data)
    # 'Offline' channel in the fixture has $500 spend but NO signup events.
    # Therefore, cac should be 500 / NULL = NULL.
    cac_val = silver_data.execute("SELECT cac FROM cac_by_channel WHERE channel = 'Offline'").fetchone()[0]
    
    assert cac_val is None, f"Expected None for CAC with 0 signups, but got {cac_val}"

def test_gold_mrr_active_only(silver_data):
    # Add a cancelled subscription
    silver_data.execute("INSERT INTO silver_subscriptions VALUES ('s2', 99.0, '2026-01-01', 'cancelled')")
    run_gold(silver_data)
    mrr = silver_data.execute("SELECT mrr FROM mrr_monthly").fetchone()[0]
    assert mrr == 50.0 # Should ignore the 99.0 cancelled sub
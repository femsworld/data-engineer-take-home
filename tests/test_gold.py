import pytest
import duckdb
from src.gold import run_gold

@pytest.fixture
def silver_data():
    con = duckdb.connect(':memory:')
    con.execute("""
        CREATE TABLE silver_events (event_id VARCHAR, user_id VARCHAR, event_type VARCHAR, event_ts TIMESTAMP, amount DOUBLE, is_bot BOOLEAN);
        INSERT INTO silver_events VALUES 
        ('e1', 'u1', 'signup', '2026-01-01', 0, false),
        ('e2', 'u1', 'purchase', '2026-01-02', 100.0, false),
        ('e3', 'u1', 'refund', '2026-01-03', 20.0, false); -- Net should be 80
        
        CREATE TABLE silver_subscriptions (subscription_id VARCHAR, price DOUBLE, created_at TIMESTAMP, status VARCHAR);
        INSERT INTO silver_subscriptions VALUES ('s1', 50.0, '2026-01-01', 'active');

        CREATE TABLE silver_marketing (date DATE, channel VARCHAR, spend DOUBLE);
        INSERT INTO silver_marketing VALUES ('2026-01-01', 'Search', 10.0);
    """)
    return con

def test_gold_revenue_calculation(silver_data):
    run_gold(silver_data)
    net_rev = silver_data.execute("SELECT SUM(net_revenue) FROM daily_revenue_net").fetchone()[0]
    assert net_rev == 80.0

def test_gold_mrr_calculation(silver_data):
    run_gold(silver_data)
    mrr = silver_data.execute("SELECT SUM(mrr) FROM mrr_monthly").fetchone()[0]
    assert mrr == 50.0
import pytest
import duckdb
from src.silver import run_silver

@pytest.fixture
def mock_con():
    """Setup an in-memory DB with dummy messy data."""
    con = duckdb.connect(':memory:')
    con.execute("""
        CREATE TABLE bronze_marketing (date VARCHAR, channel VARCHAR, spend VARCHAR);
        INSERT INTO bronze_marketing VALUES ('2026-01-01', 'Google', '100'), ('2026-01-01', 'Google', '-50');

        CREATE TABLE bronze_subscriptions (subscription_id VARCHAR, price DOUBLE, created_at TIMESTAMP, status VARCHAR);
        INSERT INTO bronze_subscriptions VALUES ('sub1', 10.0, '2026-01-01', 'active'), (NULL, 20.0, '2026-01-01', 'active');

        CREATE TABLE bronze_events (event_id VARCHAR, user_id VARCHAR, event_type VARCHAR, timestamp VARCHAR, amount VARCHAR, currency VARCHAR, refers_to_event_id VARCHAR);
        -- Insert a 'ten' trap and a bot burst (21 events for user 'bot_1' at same second)
        INSERT INTO bronze_events SELECT 'ev_1', 'u1', 'purchase', '2026-01-01 10:00:00', '10.5', 'USD', NULL;
        INSERT INTO bronze_events SELECT 'ev_bad', 'u2', 'purchase', '2026-01-01 10:00:00', 'ten', 'USD', NULL;
        
        INSERT INTO bronze_events 
        SELECT 'bot_ev_' || range, 'bot_1', 'page_view', '2026-01-01 11:00:00', NULL, NULL, NULL 
        FROM range(25);
    """)
    return con

def test_silver_bot_detection(mock_con):
    run_silver(mock_con)
    is_bot = mock_con.execute("SELECT is_bot FROM silver_events WHERE user_id = 'bot_1' LIMIT 1").fetchone()[0]
    assert is_bot is True

def test_silver_quarantine_ten_trap(mock_con):
    run_silver(mock_con)
    # Check if 'ten' ended up in quarantine
    quarantine_count = mock_con.execute("SELECT COUNT(*) FROM quarantine_events WHERE amount = 'ten'").fetchone()[0]
    assert quarantine_count == 1
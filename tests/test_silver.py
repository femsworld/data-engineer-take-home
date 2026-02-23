import pytest
import duckdb
from src.silver import run_silver

@pytest.fixture
def mock_con():
    """Setup an in-memory DB with dummy messy data representing all known traps."""
    con = duckdb.connect(':memory:')
    
    # 1. Setup Bronze Marketing (Testing Duplicates and Negative Spend)
    con.execute("CREATE TABLE bronze_marketing (date VARCHAR, channel VARCHAR, spend VARCHAR);")
    con.execute("""
        INSERT INTO bronze_marketing VALUES 
        ('2026-01-01', 'Google', '100'), 
        ('2026-01-01', 'Google', '100'), -- Exact duplicate
        ('2026-01-01', 'Google', '-50'), -- Negative spend trap
        ('2026-01-01', 'Facebook', 'ten'); -- Non-numeric trap
    """)

    # 2. Setup Bronze Subscriptions
    con.execute("CREATE TABLE bronze_subscriptions (subscription_id VARCHAR, price DOUBLE, created_at TIMESTAMP, status VARCHAR);")
    con.execute("""
        INSERT INTO bronze_subscriptions VALUES 
        ('sub1', 10.0, '2026-01-01', 'active'), 
        (NULL, 20.0, '2026-01-01', 'active'); -- Missing ID trap
    """)

    # 3. Setup Bronze Events (Testing "Ten", Bots, and Timestamp Formats)
    con.execute("CREATE TABLE bronze_events (event_id VARCHAR, user_id VARCHAR, event_type VARCHAR, timestamp VARCHAR, amount VARCHAR, currency VARCHAR, refers_to_event_id VARCHAR);")
    con.execute("""
        -- Standard row
        INSERT INTO bronze_events SELECT 'ev_1', 'u1', 'purchase', '2026-01-01 10:00:00', '10.5', 'USD', NULL;
        -- The 'ten' trap
        INSERT INTO bronze_events SELECT 'ev_bad', 'u2', 'purchase', '2026-01-01 10:00:00', 'ten', 'USD', NULL;
        -- Mixed Timestamp Formats
        INSERT INTO bronze_events SELECT 'ts_1', 'u3', 'signup', '2026-01-01T10:00:00Z', NULL, NULL, NULL;
        INSERT INTO bronze_events SELECT 'ts_2', 'u3', 'page_view', '2026-01-01T10:00:00+00:00', NULL, NULL, NULL;
        
        -- Bot Burst (25 events in 1 second)
        INSERT INTO bronze_events 
        SELECT 'bot_ev_' || range, 'bot_1', 'page_view', '2026-01-02 11:00:00', NULL, NULL, NULL 
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
    count = mock_con.execute("SELECT COUNT(*) FROM quarantine_events WHERE rejection_reason LIKE '%ten%'").fetchone()[0]
    assert count == 1

def test_silver_marketing_deduplication(mock_con):
    run_silver(mock_con)
    # Original had 4 rows, 1 negative, 1 non-numeric, 1 exact duplicate. Only 1 should remain.
    clean_count = mock_con.execute("SELECT COUNT(*) FROM silver_marketing").fetchone()[0]
    assert clean_count == 1

def test_silver_timestamp_normalization(mock_con):
    run_silver(mock_con)
    # Check that diverse timestamp formats were converted to actual TIMESTAMP types (not null)
    null_ts = mock_con.execute("SELECT COUNT(*) FROM silver_events WHERE event_ts IS NULL").fetchone()[0]
    assert null_ts == 0
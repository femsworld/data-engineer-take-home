import duckdb
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

def run_bronze(con):
    """
    Ingests raw data into the Bronze layer using high-performance DuckDB native readers.
    
    This function implements a 'Schema-on-Read' strategy:
    1. Marketing Spend: Forced to VARCHAR to prevent premature type-casting errors.
    2. Subscriptions: Loaded via native JSON reader.
    3. Events (NDJSON): Uses DuckDB's C++ engine with explicit column mapping. 
       By mapping all columns to VARCHAR, we ensure 'user_id' is present and the 
       'amount' trap ("ten") is safely ingested as text.
    """
    print("--- Starting Bronze Layer: Ingestion ---")
    
    # 1. Marketing Spend
    mkt_path = os.path.join(DATA_DIR, 'marketing_spend.csv')
    con.execute(f"CREATE OR REPLACE TABLE bronze_marketing AS SELECT * FROM read_csv_auto('{mkt_path}', all_varchar=True)")
    
    # 2. Subscriptions
    sub_path = os.path.join(DATA_DIR, 'subscriptions.json')
    con.execute(f"CREATE OR REPLACE TABLE bronze_subscriptions AS SELECT * FROM read_json_auto('{sub_path}')")

    # 3. Events: Optimized High-Performance Load
    event_path = os.path.join(DATA_DIR, 'events.ndjson')
    
    # We define the full schema as VARCHAR to ensure no columns are dropped 
    # and no data-type errors (like 'ten') stop the ingestion.
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_events AS 
        SELECT * FROM read_json_auto(
            '{event_path}', 
            ignore_errors=True, 
            format='newline_delimited',
            columns={{
                'event_id': 'VARCHAR',
                'user_id': 'VARCHAR',
                'event_type': 'VARCHAR',
                'timestamp': 'VARCHAR',
                'amount': 'VARCHAR',
                'currency': 'VARCHAR',
                'refers_to_event_id': 'VARCHAR'
            }}
        )
    """)

    # Calculate Corruption
    total_lines = sum(1 for _ in open(event_path, 'r', encoding='utf-8'))
    loaded_rows = con.execute("SELECT count(*) FROM bronze_events").fetchone()[0]
    corrupted_count = total_lines - loaded_rows
    
    print(f"Bronze complete. Quarantined {corrupted_count} corrupted rows.")
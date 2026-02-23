import duckdb
import os
import time
from bronze import run_bronze
from silver import run_silver
from gold import run_gold

def run_full_pipeline():
    """
    Main orchestrator for the Audicin Data Lakehouse pipeline.
    
    This function manages the end-to-end lifecycle of the data:
    1. Establishes a connection to the local DuckDB instance.
    2. Executes the Bronze (Ingestion), Silver (Cleaning), and Gold (Analytics) layers.
    3. Profiles the total execution time for performance monitoring.
    4. Ensures the database connection is gracefully closed upon completion.
    """
    # Ensure DB path is consistent across different environments
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(BASE_DIR, 'audicin_lakehouse.db')
    
    # Start the timer
    start_time = time.time()
    
    try:
        con = duckdb.connect(db_path)
        print(f"Connected to {db_path}")

        # Run the Medallion steps in sequence
        run_bronze(con)
        run_silver(con)
        run_gold(con)
        
        # Calculate duration
        end_time = time.time()
        duration = end_time - start_time
        
        print("\n" + "="*30)
        print("--- Pipeline Success ---")
        print(f"Total Execution Time: {duration:.2f} seconds")
        print("="*30)
        
    except Exception as e:
        print(f"\n!!! Pipeline Failed: {e}")
        raise
    finally:
        con.close()

if __name__ == "__main__":
    run_full_pipeline()
import duckdb
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(BASE_DIR, 'audicin_lakehouse.db')

def list_lakehouse_tables():
    conn = duckdb.connect(db_path)
    
    query = """
    SELECT 
        table_name, 
        column_count, 
        estimated_size as row_count_est
    FROM duckdb_tables()
    ORDER BY table_name;
    """
    
    print("\n" + "="*20 + " LAKEHOUSE INVENTORY " + "="*20)
    print(conn.execute(query).df().to_string(index=False))
    conn.close()

if __name__ == "__main__":
    list_lakehouse_tables()
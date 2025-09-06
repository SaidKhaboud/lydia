import sys
import duckdb


def verify_raw_table():
    """Verify that the market_chart table exists and has valid data"""

    db_path = "/opt/airflow/db/db_crypto_data.duckdb"
    table_name = "market_chart"
    conn = duckdb.connect(db_path)
    try:
        # Check if the table exists
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        if table_name not in tables:
            print(f"Table '{table_name}' not found in the database.")
            sys.exit(1)
        # Check if table is not empty
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        if count == 0:
            print(f"Table '{table_name}' is empty.")
            sys.exit(1)
        # Check if any row has prices array length > 1 (prices is JSON)
        result = conn.execute(f"SELECT prices FROM {table_name} WHERE json_array_length(prices) > 1 LIMIT 1").fetchone()
        if result is None:
            print(f"No row in '{table_name}' has prices array length > 1.")
            sys.exit(1)
        print(f"Test passed: Table '{table_name}' is not empty and has a prices array with length > 1.")
        sys.exit(0)
    finally:
        conn.close()


if __name__ == "__main__":
    verify_raw_table()

import psycopg2
from tabulate import tabulate

# === Configuration ===
DB_CONFIG = {
    "dbname": "ecommerce_analytics",
    "user": "postgres",
    "password": "2409",  # Change this to your actual password
    "host": "localhost",
    "port": "5432"
}
SCHEMA_NAME = "dbt_dev"


def get_tables(cursor, schema):
    cursor.execute("""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = %s;
    """, (schema,))
    return [row[0] for row in cursor.fetchall()]


def get_table_preview(cursor, schema, table, limit=10):
    query = f"SELECT * FROM \"{schema}\".\"{table}\" LIMIT {limit};"
    cursor.execute(query)
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    return headers, rows


def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print(f"\n🔍 Connecting to schema '{SCHEMA_NAME}' in database '{DB_CONFIG['dbname']}'...\n")

        tables = get_tables(cursor, SCHEMA_NAME)

        if not tables:
            print(f"❌ No tables found in schema '{SCHEMA_NAME}'.")
            return

        for table in tables:
            print(f"\n===== 📄 First 10 rows of {SCHEMA_NAME}.{table} =====")
            try:
                headers, rows = get_table_preview(cursor, SCHEMA_NAME, table)
                print(tabulate(rows, headers=headers, tablefmt="pretty"))
            except Exception as e:
                print(f"❌ Error fetching data from {table}: {e}")

        print("\n✅ Done inspecting tables.")

    except psycopg2.OperationalError as e:
        print(f"❌ Database connection failed: {e}")
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    main()
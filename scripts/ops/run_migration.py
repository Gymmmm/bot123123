
import sqlite3
import os

def run_migration(db_path, sql_file_path):
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        with open(sql_file_path, 'r') as f:
            sql_script = f.read()

        # Split the script into individual statements
        statements = [s.strip() for s in sql_script.split(';') if s.strip()]

        for statement in statements:
            try:
                cursor.execute(statement)
                conn.commit() # Commit after each statement
                print(f"Executed: {statement[:70]}...")
            except sqlite3.OperationalError as e:
                print(f"Error executing statement: {statement[:70]}... - {e}")
                # Continue if it's an 'already exists' error for IF NOT EXISTS clauses
                if "already exists" not in str(e):
                    raise e

        print("Migration completed successfully.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    DB_PATH = os.getenv("DB_PATH", "data/qiaolian_dual_bot.db")
    SQL_FILE_PATH = os.getenv("MIGRATION_SQL", "migration.sql")
    run_migration(DB_PATH, SQL_FILE_PATH)

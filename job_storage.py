import sqlite3
from datetime import datetime

DB_PATH = "queuectl.db"

# Initialize DB and create table if it doesn’t exist
def init_db(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
    ''')
    conn.commit()
    conn.close()

# Helper: Get current UTC time as string
def current_time():
    return datetime.utcnow().isoformat() + "Z"

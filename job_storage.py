import sqlite3
from datetime import datetime

DB_PATH = "queuectl.db"

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

def current_time():
    return datetime.utcnow().isoformat() + "Z"

def add_job(job):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    sql = '''
        INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    '''
    cursor.execute(sql, (
        job['id'], job['command'], job['state'],
        job['attempts'], job['max_retries'],
        job['created_at'], job['updated_at']
    ))
    conn.commit()
    conn.close()

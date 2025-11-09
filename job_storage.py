import sqlite3
from datetime import datetime, timedelta
import os
import time


DB_PATH = os.path.join(os.getcwd(), "queuectl.db")


def current_time():
    return datetime.utcnow().isoformat() + "Z"


def _now_iso(dt=None):
    if dt is None:
        return datetime.utcnow().isoformat() + "Z"
    return dt.isoformat() + "Z"


def _get_conn(db_path=DB_PATH):
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    try:
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA busy_timeout=30000;')
    except Exception:
        pass
    return conn


def init_db(db_path=DB_PATH):
    conn = _get_conn(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    conn.commit()

    cursor.execute("PRAGMA table_info(jobs)")
    cols = [r[1] for r in cursor.fetchall()]
    if 'next_run_at' not in cols:
        try:
            cursor.execute("ALTER TABLE jobs ADD COLUMN next_run_at TEXT")
            conn.commit()
        except Exception:
            pass
    conn.close()


def add_job(job, db_path=DB_PATH):
    conn = _get_conn(db_path)
    cursor = conn.cursor()
    sql = (
        "INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    cursor.execute(sql, (
        job['id'], job['command'], job['state'],
        job['attempts'], job['max_retries'],
        job['created_at'], job['updated_at'], job.get('next_run_at')
    ))
    conn.commit()
    conn.close()


def list_jobs_by_state(state=None, db_path=DB_PATH):
    conn = _get_conn(db_path)
    cursor = conn.cursor()
    if state:
        cursor.execute(
            'SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs WHERE state = ? ORDER BY created_at',
            (state,)
        )
    else:
        cursor.execute('SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs ORDER BY created_at')
    rows = cursor.fetchall()
    conn.close()
    jobs = []
    for r in rows:
        jobs.append({
            'id': r[0], 'command': r[1], 'state': r[2], 'attempts': r[3],
            'max_retries': r[4], 'created_at': r[5], 'updated_at': r[6], 'next_run_at': r[7]
        })
    return jobs


def get_stats(db_path=DB_PATH):
    conn = _get_conn(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
    rows = cursor.fetchall()
    conn.close()
    stats = {r[0]: r[1] for r in rows}
    return stats


def _retry_on_lock(func, retries=5, backoff=0.05):
    for attempt in range(retries):
        try:
            return func()
        except sqlite3.OperationalError as e:
            if 'locked' in str(e).lower() and attempt + 1 < retries:
                time.sleep(backoff * (attempt + 1))
                continue
            raise


def claim_job(db_path=DB_PATH):
    def _work():
        conn = _get_conn(db_path)
        cursor = conn.cursor()
        now = _now_iso()
        try:
            cursor.execute('BEGIN IMMEDIATE')
            cursor.execute(
                "SELECT id, command, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs WHERE state = 'pending' AND (next_run_at IS NULL OR next_run_at <= ?) ORDER BY created_at LIMIT 1",
                (now,)
            )
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                return None
            job_id = row[0]
            cursor.execute("UPDATE jobs SET state = 'processing', updated_at = ? WHERE id = ? AND state = 'pending'", (now, job_id))
            if cursor.rowcount != 1:
                conn.rollback()
                return None
            conn.commit()
            return {
                'id': row[0], 'command': row[1], 'attempts': row[2], 'max_retries': row[3],
                'created_at': row[4], 'updated_at': row[5], 'next_run_at': row[6]
            }
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return None
        finally:
            conn.close()

    return _retry_on_lock(_work)


def mark_job_completed(job_id, db_path=DB_PATH):
    def _work():
        conn = _get_conn(db_path)
        cursor = conn.cursor()
        now = _now_iso()
        cursor.execute("UPDATE jobs SET state = 'completed', updated_at = ? WHERE id = ?", (now, job_id))
        conn.commit()
        conn.close()

    return _retry_on_lock(_work)


def mark_job_failed(job_id, attempts, max_retries, backoff_base=2, db_path=DB_PATH):
    def _work():
        conn = _get_conn(db_path)
        cursor = conn.cursor()
        now_dt = datetime.utcnow()
        attempts_local = attempts + 1
        if attempts_local > max_retries:
            cursor.execute("UPDATE jobs SET state = 'dead', attempts = ?, updated_at = ? WHERE id = ?", (attempts_local, _now_iso(now_dt), job_id))
        else:
            delay = (backoff_base ** attempts_local)
            next_run = now_dt + timedelta(seconds=delay)
            cursor.execute("UPDATE jobs SET attempts = ?, state = 'pending', next_run_at = ?, updated_at = ? WHERE id = ?", (attempts_local, _now_iso(next_run), _now_iso(now_dt), job_id))
        conn.commit()
        conn.close()

    return _retry_on_lock(_work)


def retry_dead_job(job_id, db_path=DB_PATH):
    def _work():
        conn = _get_conn(db_path)
        cursor = conn.cursor()
        now = _now_iso()
        cursor.execute("UPDATE jobs SET state = 'pending', attempts = 0, next_run_at = NULL, updated_at = ? WHERE id = ? AND state = 'dead'", (now, job_id))
        conn.commit()
        conn.close()

    return _retry_on_lock(_work)
import sqlite3
from datetime import datetime, timedelta
import os
import time


DB_PATH = os.path.join(os.getcwd(), "queuectl.db")


def current_time():
    return datetime.utcnow().isoformat() + "Z"


def _now_iso(dt=None):
    if dt is None:
        return datetime.utcnow().isoformat() + "Z"
    return dt.isoformat() + "Z"


def _get_conn(db_path=DB_PATH):
    # centralize connection options
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    # enable WAL for better concurrency and set busy timeout
    try:
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA busy_timeout=30000;')
    except Exception:
        pass
    return conn


def init_db(db_path=DB_PATH):
    conn = _get_conn(db_path)
    cursor = conn.cursor()
    # create table if missing
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

    # migration: ensure next_run_at column exists
    cursor.execute("PRAGMA table_info(jobs)")
    cols = [r[1] for r in cursor.fetchall()]
    if 'next_run_at' not in cols:
        try:
            cursor.execute("ALTER TABLE jobs ADD COLUMN next_run_at TEXT")
            conn.commit()
        except Exception:
            # if alter fails, ignore; table may be locked or migration unnecessary
            pass
    conn.close()


def add_job(job, db_path=DB_PATH):
    conn = _get_conn(db_path)
    cursor = conn.cursor()
    sql = '''
        INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    '''
    cursor.execute(sql, (
        job['id'], job['command'], job['state'],
        job['attempts'], job['max_retries'],
        job['created_at'], job['updated_at'], job.get('next_run_at')
    ))
    conn.commit()
    conn.close()


def list_jobs_by_state(state=None, db_path=DB_PATH):
    conn = _get_conn(db_path)
    cursor = conn.cursor()
    if state:
        cursor.execute('SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs WHERE state = ? ORDER BY created_at', (state,))
    else:
        cursor.execute('SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs ORDER BY created_at')
    rows = cursor.fetchall()
    conn.close()
    jobs = []
    for r in rows:
        jobs.append({
            'id': r[0], 'command': r[1], 'state': r[2], 'attempts': r[3],
            'max_retries': r[4], 'created_at': r[5], 'updated_at': r[6], 'next_run_at': r[7]
        })
    return jobs


def get_stats(db_path=DB_PATH):
    conn = _get_conn(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
    rows = cursor.fetchall()
    conn.close()
    stats = {r[0]: r[1] for r in rows}
    return stats


def _retry_on_lock(func, retries=5, backoff=0.05):
    for attempt in range(retries):
        try:
            return func()
        except sqlite3.OperationalError as e:
            if 'locked' in str(e).lower() and attempt + 1 < retries:
                time.sleep(backoff * (attempt + 1))
                continue
            raise


def claim_job(db_path=DB_PATH):
    """Atomically pick one pending job whose next_run_at is null or <= now and mark it processing.
    Returns the job dict or None."""

    def _work():
        conn = _get_conn(db_path)
        cursor = conn.cursor()
        now = _now_iso()
        try:
            cursor.execute('BEGIN IMMEDIATE')
            cursor.execute("SELECT id, command, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs WHERE state = 'pending' AND (next_run_at IS NULL OR next_run_at <= ?) ORDER BY created_at LIMIT 1", (now,))
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                return None
            job_id = row[0]
            cursor.execute("UPDATE jobs SET state = 'processing', updated_at = ? WHERE id = ? AND state = 'pending'", (now, job_id))
            if cursor.rowcount != 1:
                conn.rollback()
                return None
            conn.commit()
            return {
                'id': row[0], 'command': row[1], 'attempts': row[2], 'max_retries': row[3],
                'created_at': row[4], 'updated_at': row[5], 'next_run_at': row[6]
            }
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return None
        finally:
            conn.close()

    return _retry_on_lock(_work)


def mark_job_completed(job_id, db_path=DB_PATH):
    def _work():
        conn = _get_conn(db_path)
        cursor = conn.cursor()
        now = _now_iso()
        cursor.execute("UPDATE jobs SET state = 'completed', updated_at = ? WHERE id = ?", (now, job_id))
        conn.commit()
        conn.close()

    return _retry_on_lock(_work)


def mark_job_failed(job_id, attempts, max_retries, backoff_base=2, db_path=DB_PATH):
    """Increment attempts and either reschedule with backoff or mark dead."""

    def _work():
        conn = _get_conn(db_path)
        cursor = conn.cursor()
        now_dt = datetime.utcnow()
        attempts_local = attempts + 1
        if attempts_local > max_retries:
            # move to dead
            cursor.execute("UPDATE jobs SET state = 'dead', attempts = ?, updated_at = ? WHERE id = ?", (attempts_local, _now_iso(now_dt), job_id))
        else:
            # schedule next run with exponential backoff (base ** attempts) seconds
            delay = (backoff_base ** attempts_local)
            next_run = now_dt + timedelta(seconds=delay)
            cursor.execute("UPDATE jobs SET attempts = ?, state = 'pending', next_run_at = ?, updated_at = ? WHERE id = ?", (attempts_local, _now_iso(next_run), _now_iso(now_dt), job_id))
        conn.commit()
        conn.close()

    return _retry_on_lock(_work)


def retry_dead_job(job_id, db_path=DB_PATH):
    def _work():
        conn = _get_conn(db_path)
        cursor = conn.cursor()
        now = _now_iso()
        cursor.execute("UPDATE jobs SET state = 'pending', attempts = 0, next_run_at = NULL, updated_at = ? WHERE id = ? AND state = 'dead'", (now, job_id))
        conn.commit()
        conn.close()

    return _retry_on_lock(_work)

import sqlite3
from datetime import datetime, timedelta
import os

DB_PATH = os.path.join(os.getcwd(), "queuectl.db")


def current_time():
    return datetime.utcnow().isoformat() + "Z"


def _now_iso(dt=None):
    if dt is None:
        return datetime.utcnow().isoformat() + "Z"
    return dt.isoformat() + "Z"


def init_db(db_path=DB_PATH):
    conn = sqlite3.connect(db_path, timeout=30)
    cursor = conn.cursor()
    # create table if missing
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        import sqlite3
        from datetime import datetime, timedelta
        import os
        import time


        DB_PATH = os.path.join(os.getcwd(), "queuectl.db")


        def current_time():
            return datetime.utcnow().isoformat() + "Z"


        def _now_iso(dt=None):
            if dt is None:
                return datetime.utcnow().isoformat() + "Z"
            return dt.isoformat() + "Z"


        def _get_conn(db_path=DB_PATH):
            # centralize connection options
            conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
            # enable WAL for better concurrency
            try:
                conn.execute('PRAGMA journal_mode=WAL;')
                conn.execute('PRAGMA busy_timeout=30000;')
            except Exception:
                pass
            return conn


        def init_db(db_path=DB_PATH):
            conn = _get_conn(db_path)
            cursor = conn.cursor()
            # create table if missing
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

            # migration: ensure next_run_at column exists
            cursor.execute("PRAGMA table_info(jobs)")
            cols = [r[1] for r in cursor.fetchall()]
            if 'next_run_at' not in cols:
                try:
                    cursor.execute("ALTER TABLE jobs ADD COLUMN next_run_at TEXT")
                    conn.commit()
                except Exception:
                    # if alter fails, ignore; table may be locked or migration unnecessary
                    pass
            conn.close()


        def add_job(job, db_path=DB_PATH):
            conn = _get_conn(db_path)
            cursor = conn.cursor()
            sql = '''
                INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''
            cursor.execute(sql, (
                job['id'], job['command'], job['state'],
                job['attempts'], job['max_retries'],
                job['created_at'], job['updated_at'], job.get('next_run_at')
            ))
            conn.commit()
            conn.close()


        def list_jobs_by_state(state=None, db_path=DB_PATH):
            conn = _get_conn(db_path)
            cursor = conn.cursor()
            if state:
                cursor.execute('SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs WHERE state = ? ORDER BY created_at', (state,))
            else:
                cursor.execute('SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs ORDER BY created_at')
            rows = cursor.fetchall()
            conn.close()
            jobs = []
            for r in rows:
                jobs.append({
                    'id': r[0], 'command': r[1], 'state': r[2], 'attempts': r[3],
                    'max_retries': r[4], 'created_at': r[5], 'updated_at': r[6], 'next_run_at': r[7]
                })
            return jobs


        def get_stats(db_path=DB_PATH):
            conn = _get_conn(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
            rows = cursor.fetchall()
            conn.close()
            stats = {r[0]: r[1] for r in rows}
            return stats


        def _retry_on_lock(func, retries=5, backoff=0.05):
            for attempt in range(retries):
                try:
                    return func()
                except sqlite3.OperationalError as e:
                    if 'locked' in str(e).lower() and attempt + 1 < retries:
                        time.sleep(backoff * (attempt + 1))
                        continue
                    raise


        def claim_job(db_path=DB_PATH):
            """Atomically pick one pending job whose next_run_at is null or <= now and mark it processing.
            Returns the job dict or None."""

            def _work():
                conn = _get_conn(db_path)
                cursor = conn.cursor()
                now = _now_iso()
                try:
                    cursor.execute('BEGIN IMMEDIATE')
                    cursor.execute("SELECT id, command, attempts, max_retries, created_at, updated_at, next_run_at FROM jobs WHERE state = 'pending' AND (next_run_at IS NULL OR next_run_at <= ?) ORDER BY created_at LIMIT 1", (now,))
                    row = cursor.fetchone()
                    if not row:
                        conn.rollback()
                        return None
                    job_id = row[0]
                    cursor.execute("UPDATE jobs SET state = 'processing', updated_at = ? WHERE id = ? AND state = 'pending'", (now, job_id))
                    if cursor.rowcount != 1:
                        conn.rollback()
                        return None
                    conn.commit()
                    return {
                        'id': row[0], 'command': row[1], 'attempts': row[2], 'max_retries': row[3],
                        'created_at': row[4], 'updated_at': row[5], 'next_run_at': row[6]
                    }
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    return None
                finally:
                    conn.close()

            return _retry_on_lock(_work)


        def mark_job_completed(job_id, db_path=DB_PATH):
            def _work():
                conn = _get_conn(db_path)
                cursor = conn.cursor()
                now = _now_iso()
                cursor.execute("UPDATE jobs SET state = 'completed', updated_at = ? WHERE id = ?", (now, job_id))
                conn.commit()
                conn.close()

            return _retry_on_lock(_work)


        def mark_job_failed(job_id, attempts, max_retries, backoff_base=2, db_path=DB_PATH):
            """Increment attempts and either reschedule with backoff or mark dead."""

            def _work():
                conn = _get_conn(db_path)
                cursor = conn.cursor()
                now_dt = datetime.utcnow()
                attempts_local = attempts + 1
                if attempts_local > max_retries:
                    # move to dead
                    cursor.execute("UPDATE jobs SET state = 'dead', attempts = ?, updated_at = ? WHERE id = ?", (attempts_local, _now_iso(now_dt), job_id))
                else:
                    # schedule next run with exponential backoff (base ** attempts) seconds
                    delay = (backoff_base ** attempts_local)
                    next_run = now_dt + timedelta(seconds=delay)
                    cursor.execute("UPDATE jobs SET attempts = ?, state = 'pending', next_run_at = ?, updated_at = ? WHERE id = ?", (attempts_local, _now_iso(next_run), _now_iso(now_dt), job_id))
                conn.commit()
                conn.close()

            return _retry_on_lock(_work)


        def retry_dead_job(job_id, db_path=DB_PATH):
            def _work():
                conn = _get_conn(db_path)
                cursor = conn.cursor()
                now = _now_iso()
                cursor.execute("UPDATE jobs SET state = 'pending', attempts = 0, next_run_at = NULL, updated_at = ? WHERE id = ? AND state = 'dead'", (now, job_id))
                conn.commit()
                conn.close()

            return _retry_on_lock(_work)


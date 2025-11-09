import os
import threading
import time
import subprocess
import tempfile
import importlib

import job_storage as store
import config


def worker_loop(db_path, poll_interval, stop_event):
    """Simple worker loop used by tests that calls storage APIs directly."""
    while not stop_event.is_set():
        job = store.claim_job(db_path=db_path)
        if not job:
            stop_event.wait(poll_interval)
            continue

        job_id = job['id']
        cmd = job['command']
        attempts = job.get('attempts', 0)
        max_retries = job.get('max_retries', 3)

        try:
            proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=None)
            rc = proc.returncode
        except Exception:
            rc = 1

        if rc == 0:
            store.mark_job_completed(job_id, db_path=db_path)
        else:
            store.mark_job_failed(job_id, attempts, max_retries, backoff_base=config.get_config('backoff_base') or 2, db_path=db_path)


def test_basic_job_completes(tmp_path):
    db_file = tmp_path / 'queuectl.db'
    db_path = str(db_file)

    # ensure config and DB are isolated in tmp dir
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        store.init_db(db_path=db_path)

        job = {
            'id': 'job-success',
            'command': 'python -c "import sys; sys.exit(0)"',
            'state': 'pending',
            'attempts': 0,
            'max_retries': 0,
            'created_at': store.current_time(),
            'updated_at': store.current_time(),
        }
        store.add_job(job, db_path=db_path)

        stop = threading.Event()
        t = threading.Thread(target=worker_loop, args=(db_path, 0.1, stop), daemon=True)
        t.start()

        # wait for completion
        for _ in range(50):
            jobs = store.list_jobs_by_state('completed', db_path=db_path)
            if any(j['id'] == 'job-success' for j in jobs):
                break
            time.sleep(0.1)

        stop.set()
        t.join(timeout=1)

        completed = store.list_jobs_by_state('completed', db_path=db_path)
        assert any(j['id'] == 'job-success' for j in completed)
    finally:
        os.chdir(cwd)


def test_failed_job_retries_and_moves_to_dlq(tmp_path):
    db_file = tmp_path / 'queuectl.db'
    db_path = str(db_file)
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        # keep backoff short for test
        config.set_config('backoff_base', 1)
        store.init_db(db_path=db_path)

        job = {
            'id': 'job-fail',
            'command': 'python -c "import sys; sys.exit(1)"',
            'state': 'pending',
            'attempts': 0,
            'max_retries': 1,
            'created_at': store.current_time(),
            'updated_at': store.current_time(),
        }
        store.add_job(job, db_path=db_path)

        stop = threading.Event()
        t = threading.Thread(target=worker_loop, args=(db_path, 0.1, stop), daemon=True)
        t.start()

        # wait for job to become dead (DLQ)
        for _ in range(80):
            dead = store.list_jobs_by_state('dead', db_path=db_path)
            if any(j['id'] == 'job-fail' for j in dead):
                break
            time.sleep(0.1)

        stop.set()
        t.join(timeout=1)

        dead = store.list_jobs_by_state('dead', db_path=db_path)
        assert any(j['id'] == 'job-fail' for j in dead)
    finally:
        os.chdir(cwd)


def test_multiple_workers_no_duplicate_processing(tmp_path):
    db_file = tmp_path / 'queuectl.db'
    db_path = str(db_file)
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        store.init_db(db_path=db_path)

        n = 5
        for i in range(n):
            job = {
                'id': f'job-{i}',
                'command': 'python -c "import time; time.sleep(0.05)"',
                'state': 'pending',
                'attempts': 0,
                'max_retries': 0,
                'created_at': store.current_time(),
                'updated_at': store.current_time(),
            }
            store.add_job(job, db_path=db_path)

        stop = threading.Event()
        workers = []
        for _ in range(3):
            t = threading.Thread(target=worker_loop, args=(db_path, 0.05, stop), daemon=True)
            t.start()
            workers.append(t)

        # wait for all to complete
        for _ in range(100):
            completed = store.list_jobs_by_state('completed', db_path=db_path)
            if len(completed) >= n:
                break
            time.sleep(0.05)

        stop.set()
        for t in workers:
            t.join(timeout=1)

        completed = store.list_jobs_by_state('completed', db_path=db_path)
        assert len(completed) == n
    finally:
        os.chdir(cwd)

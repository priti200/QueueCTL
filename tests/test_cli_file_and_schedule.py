import os
import subprocess
import threading
import time
import sys
from pathlib import Path

import job_storage as store
import config


def run_cli(args, cwd):
    python = sys.executable
    repo_root = Path(__file__).resolve().parents[1]
    main_py = str(repo_root / 'main.py')
    args_resolved = [main_py if a == 'main.py' else a for a in args]
    env = os.environ.copy()
    env['QUEUECTL_DB_PATH'] = store.DB_PATH
    env['QUEUECTL_CONFIG_PATH'] = config._CFG_PATH
    return subprocess.check_output([python] + args_resolved, text=True, cwd=str(cwd), stderr=subprocess.STDOUT, env=env)


def start_test_worker(db_path, stop_event, poll_interval=0.1):
    def runner():
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
                timeout_val = config.get_config('job_timeout')
                try:
                    timeout = None if not timeout_val else float(timeout_val)
                except Exception:
                    timeout = None
                proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
                rc = proc.returncode
            except Exception:
                rc = 1
            if rc == 0:
                store.mark_job_completed(job_id, db_path=db_path)
            else:
                store.mark_job_failed(job_id, attempts, max_retries, backoff_base=config.get_config('backoff_base') or 2, db_path=db_path)

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    return t


def test_enqueue_with_command_file(tmp_path):
    cwd = tmp_path
    os.chdir(cwd)
    try:
        store.DB_PATH = os.path.join(cwd, 'queuectl.db')
        config._CFG_PATH = os.path.join(cwd, 'queuectl_config.json')
        store.init_db(db_path=store.DB_PATH)

        # create a command file
        cmdfile = cwd / 'cmd.txt'
        cmdfile.write_text('python -c "import sys; sys.exit(0)"')

        out = run_cli(['main.py', 'enqueue', '--command-file', str(cmdfile), '--max-retries', '1'], cwd)
        assert out.strip().startswith('Enqueued job:')
        job_id = out.strip().split()[-1]

        stop = threading.Event()
        t = start_test_worker(store.DB_PATH, stop)

        for _ in range(50):
            completed = store.list_jobs_by_state('completed', db_path=store.DB_PATH)
            if any(j['id'] == job_id for j in completed):
                break
            time.sleep(0.1)

        stop.set()
        t.join(timeout=1)

        completed = store.list_jobs_by_state('completed', db_path=store.DB_PATH)
        assert any(j['id'] == job_id for j in completed)
    finally:
        os.chdir('..')


def test_enqueue_job_file_and_delay(tmp_path):
    cwd = tmp_path
    os.chdir(cwd)
    try:
        store.DB_PATH = os.path.join(cwd, 'queuectl.db')
        config._CFG_PATH = os.path.join(cwd, 'queuectl_config.json')
        store.init_db(db_path=store.DB_PATH)

        # create a job JSON file
        job = {
            'id': 'job-file-1',
            'command': 'python -c "import sys; sys.exit(0)"',
            'max_retries': 1
        }
        jobfile = cwd / 'job.json'
        jobfile.write_text(__import__('json').dumps(job))

        out = run_cli(['main.py', 'enqueue', '--job-file', str(jobfile)], cwd)
        assert out.strip().startswith('Enqueued job:')
        job_id = out.strip().split()[-1]

        # schedule another job with a short delay
        out2 = run_cli(['main.py', 'enqueue', '--command', 'python -c "import sys; sys.exit(0)"', '--delay', '1'], cwd)
        job2 = out2.strip().split()[-1]

        stop = threading.Event()
        t = start_test_worker(store.DB_PATH, stop)

        # job_id (from jobfile) should be processed
        for _ in range(50):
            completed = store.list_jobs_by_state('completed', db_path=store.DB_PATH)
            if any(j['id'] == job_id for j in completed):
                break
            time.sleep(0.1)

        # job2 is scheduled with delay 1s; ensure it eventually runs
        for _ in range(80):
            completed = store.list_jobs_by_state('completed', db_path=store.DB_PATH)
            if any(j['id'] == job2 for j in completed):
                break
            time.sleep(0.1)

        stop.set()
        t.join(timeout=1)

        completed = store.list_jobs_by_state('completed', db_path=store.DB_PATH)
        ids = {j['id'] for j in completed}
        assert job_id in ids
        assert job2 in ids
    finally:
        os.chdir('..')

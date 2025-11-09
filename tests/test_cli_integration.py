import os
import subprocess
import threading
import time
import sys
from pathlib import Path

import job_storage as store
import config
import worker as worker_mod


def run_cli(args, cwd, timeout=5):
    """Run the repo's main.py with the current Python executable.

    Tests pass args lists that may include 'main.py' as the first element; replace
    that with the absolute path to the main.py in the repository root.
    """
    python = sys.executable
    repo_root = Path(__file__).resolve().parents[1]
    main_py = str(repo_root / 'main.py')

    # replace occurrences of 'main.py' in args with absolute path
    args_resolved = [main_py if a == 'main.py' else a for a in args]
    # ensure subprocess uses the same DB path and config file
    env = os.environ.copy()
    env['QUEUECTL_DB_PATH'] = store.DB_PATH
    env['QUEUECTL_CONFIG_PATH'] = config._CFG_PATH
    return subprocess.check_output([python] + args_resolved, text=True, cwd=str(cwd), stderr=subprocess.STDOUT, env=env)


def start_worker_thread(cwd, poll_interval=0.1):
    """Start a simple test worker loop that uses the same DB file."""
    stop = threading.Event()

    def runner():
        while not stop.is_set():
            job = store.claim_job(db_path=store.DB_PATH)
            if not job:
                stop.wait(poll_interval)
                continue

            job_id = job['id']
            cmd = job['command']
            attempts = job.get('attempts', 0)
            max_retries = job.get('max_retries', 3)

            try:
                # respect configured job timeout if present
                timeout_val = config.get_config('job_timeout')
                try:
                    if timeout_val in (None, 0, '0', ''):
                        timeout = None
                    else:
                        timeout = float(timeout_val)
                except Exception:
                    timeout = None

                proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
                rc = proc.returncode
            except Exception:
                rc = 1

            if rc == 0:
                store.mark_job_completed(job_id, db_path=store.DB_PATH)
            else:
                store.mark_job_failed(job_id, attempts, max_retries, backoff_base=config.get_config('backoff_base') or 2, db_path=store.DB_PATH)

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    return stop, t


def test_cli_enqueue_and_list(tmp_path):
    cwd = tmp_path
    os.chdir(cwd)
    try:
        # point storage and config to the test directory so subprocess and workers use same files
        store.DB_PATH = os.path.join(cwd, 'queuectl.db')
        config._CFG_PATH = os.path.join(cwd, 'queuectl_config.json')
        # initialize DB for the subprocess CLI to use
        store.init_db(db_path=os.path.join(cwd, 'queuectl.db'))

        # enqueue via CLI
        out = run_cli(['main.py', 'enqueue', '--command', 'python -c "import sys; sys.exit(0)"', '--max-retries', '1'], cwd)
        assert out.startswith('Enqueued job:')
        job_id = out.strip().split()[-1]

        # list via CLI
        out2 = run_cli(['main.py', 'list', '--state', 'pending'], cwd)
        assert job_id in out2

        # start worker to process it
        stop, t = start_worker_thread(cwd)
        for _ in range(50):
            completed = store.list_jobs_by_state('completed', db_path=os.path.join(cwd, 'queuectl.db'))
            if any(j['id'] == job_id for j in completed):
                break
            time.sleep(0.1)
        stop.set()
        t.join(timeout=1)

        completed = store.list_jobs_by_state('completed', db_path=os.path.join(cwd, 'queuectl.db'))
        assert any(j['id'] == job_id for j in completed)
    finally:
        os.chdir('..')


def test_dlq_retry_via_cli(tmp_path):
    cwd = tmp_path
    os.chdir(cwd)
    try:
        # ensure short backoff and 0 retries to force dead
        # point storage and config to the test directory so subprocess and workers use same files
        store.DB_PATH = os.path.join(cwd, 'queuectl.db')
        config._CFG_PATH = os.path.join(cwd, 'queuectl_config.json')
        config.set_config('backoff_base', 1)
        # initialize DB for the subprocess CLI to use
        store.init_db(db_path=os.path.join(cwd, 'queuectl.db'))

        # enqueue a failing job via CLI
        out = run_cli(['main.py', 'enqueue', '--command', 'python -c "import sys; sys.exit(1)"', '--max-retries', '0'], cwd)
        job_id = out.strip().split()[-1]

        # process it with an internal worker
        stop, t = start_worker_thread(cwd)
        for _ in range(80):
            dead = store.list_jobs_by_state('dead', db_path=os.path.join(cwd, 'queuectl.db'))
            if any(j['id'] == job_id for j in dead):
                break
            time.sleep(0.1)
        stop.set()
        t.join(timeout=1)

        dead = store.list_jobs_by_state('dead', db_path=os.path.join(cwd, 'queuectl.db'))
        assert any(j['id'] == job_id for j in dead)

        # call CLI dlq retry
        out_retry = run_cli(['main.py', 'dlq', 'retry', job_id], cwd)
        assert 'Retried job' in out_retry

        # job should now be pending
        pending = store.list_jobs_by_state('pending', db_path=os.path.join(cwd, 'queuectl.db'))
        assert any(j['id'] == job_id for j in pending)
    finally:
        os.chdir('..')


def test_job_timeout_behavior(tmp_path):
    cwd = tmp_path
    os.chdir(cwd)
    try:
        # set job timeout to 1 second
        # point storage and config to the test directory so subprocess and workers use same files
        store.DB_PATH = os.path.join(cwd, 'queuectl.db')
        config._CFG_PATH = os.path.join(cwd, 'queuectl_config.json')
        config.set_config('job_timeout', 1)
        store.init_db(db_path=os.path.join(cwd, 'queuectl.db'))

        # add a job that sleeps for 3 seconds (will be timed out)
        out = run_cli(['main.py', 'enqueue', '--command', 'python -c "import time; time.sleep(3)"', '--max-retries', '1'], cwd)
        job_id = out.strip().split()[-1]

        # start Worker (real Worker uses config.get_config for timeout)
        stop = threading.Event()
        w = worker_mod.Worker(shutdown_event=stop, poll_interval=0.1)
        w.daemon = True
        w.start()

        # wait for job to be retried (attempts increase or moved to dead)
        for _ in range(80):
            dead = store.list_jobs_by_state('dead', db_path=os.path.join(cwd, 'queuectl.db'))
            failed = store.list_jobs_by_state('failed', db_path=os.path.join(cwd, 'queuectl.db'))
            pending = store.list_jobs_by_state('pending', db_path=os.path.join(cwd, 'queuectl.db'))
            if any(j['id'] == job_id for j in dead) or any(j['id'] == job_id for j in pending) or any(j['id'] == job_id for j in failed):
                break
            time.sleep(0.1)

        stop.set()
        w.join(timeout=1)

        # ensure at least one attempt was recorded (timeout caused a failure and retry)
        all_jobs = store.list_jobs_by_state(None, db_path=os.path.join(cwd, 'queuectl.db'))
        js = [j for j in all_jobs if j['id'] == job_id]
        assert len(js) == 1
        assert js[0]['attempts'] >= 1
    finally:
        os.chdir('..')

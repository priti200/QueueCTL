import threading
import time
import subprocess
import signal
import os
from multiprocessing import Process

import job_storage as store
import config


class Worker(threading.Thread):
    def __init__(self, shutdown_event, poll_interval=1.0):
        super().__init__()
        self.shutdown_event = shutdown_event
        self.poll_interval = poll_interval

    def run(self):
        while not self.shutdown_event.is_set():
            job = store.claim_job()
            if not job:
                # nothing to do; wait a bit
                self.shutdown_event.wait(self.poll_interval)
                continue

            job_id = job['id']
            cmd = job['command']
            attempts = job.get('attempts', 0)
            max_retries = job.get('max_retries', 3)

            # run the command in a shell, capture output and apply timeout
            timeout_val = config.get_config('job_timeout')
            if not timeout_val:
                timeout_val = None
            try:
                proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout_val)
                rc = proc.returncode
                out = proc.stdout
                err = proc.stderr
            except subprocess.TimeoutExpired as e:
                rc = 1
                out = getattr(e, 'stdout', '') or ''
                err = getattr(e, 'stderr', '') or ''
            except Exception as e:
                rc = 1
                out = ''
                err = str(e)

            # store outputs to a simple log file per job (append)
            try:
                log_dir = os.path.join(os.getcwd(), 'job_logs')
                os.makedirs(log_dir, exist_ok=True)
                with open(os.path.join(log_dir, f"{job_id}.log"), 'a', encoding='utf-8') as f:
                    f.write(f"--- {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} attempt={attempts+1} rc={rc}\n")
                    if out:
                        f.write("OUT:\n" + out + "\n")
                    if err:
                        f.write("ERR:\n" + err + "\n")
            except Exception:
                # don't fail job for logging issues
                pass

            if rc == 0:
                store.mark_job_completed(job_id)
            else:
                store.mark_job_failed(job_id, attempts, max_retries, backoff_base=config.get_config('backoff_base'))


def _run_foreground(count=1, poll_interval=1.0):
    shutdown = threading.Event()

    def handle_sigint(sig, frame):
        shutdown.set()

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    threads = []
    for i in range(count):
        w = Worker(shutdown_event=shutdown, poll_interval=poll_interval)
        w.daemon = True
        w.start()
        threads.append(w)

    try:
        while not shutdown.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        shutdown.set()

    for t in threads:
        t.join()


def _run_process_worker(poll_interval=1.0):
    # helper run loop for a single process worker (used by Process target)
    shutdown = threading.Event()
    w = Worker(shutdown_event=shutdown, poll_interval=poll_interval)
    w.daemon = False
    w.start()
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        shutdown.set()
        w.join()


def start_workers(count=1, poll_interval=1.0, use_processes=False):
    """Start worker(s). If use_processes is True, spawn separate processes (one per worker).
    Otherwise spawn threads in the current process."""
    if use_processes and count > 1:
        procs = []
        for i in range(count):
            p = Process(target=_run_process_worker, args=(poll_interval,), daemon=False)
            p.start()
            procs.append(p)

        try:
            # parent waits until children exit
            while any(p.is_alive() for p in procs):
                time.sleep(0.5)
        except KeyboardInterrupt:
            for p in procs:
                try:
                    p.terminate()
                except Exception:
                    pass
        finally:
            for p in procs:
                p.join()
    else:
        # single-process threaded workers
        _run_foreground(count=count, poll_interval=poll_interval)

# queuectl

>A minimal, CLI-first background job queue with workers, retries (exponential backoff), and a Dead Letter Queue (DLQ).

This repository implements a small production-friendly job queue in Python. It supports enqueueing shell commands, running multiple workers (threads/processes), retry/backoff, persistent storage (SQLite), and CLI controls for listing, DLQ inspection and retry, and configuration.

---

## Quick links

- CLI entrypoint: `main.py`
- Worker logic: `worker.py`
- Persistence: `job_storage.py` (SQLite)
- DLQ wrapper: `dead_letter_queue.py`
- Config: `config.py` (JSON file)
- Tests: `tests/` (pytest)

---

## Requirements

- Python 3.8+ (tested on 3.10)
- pip
- Recommended: create and use a virtual environment

Install dependencies:

```powershell
pip install -r requirements.txt
```

---

## Setup

Initialize the database (creates `queuectl.db` in the current working directory):

```powershell
python main.py init
```

You can override the database path using the environment variable `QUEUECTL_DB_PATH`:

```powershell
$env:QUEUECTL_DB_PATH = "C:\path\to\queuectl.db"
python main.py init
```

Config (defaults are stored in `queuectl_config.json`):
- `max_retries` (default 3)
- `backoff_base` (default 2)
- `job_timeout` (seconds, default 0 meaning no timeout)

Set/get config with the CLI:

```powershell
python main.py config set backoff_base 2
python main.py config get backoff_base
```

---

## Usage examples

Note about PowerShell quoting: commands containing semicolons or nested quotes can be tricky. Two robust approaches are:

1. Put the script in a file and enqueue a simple `python file.py` command.
2. Use the stop-parsing operator `--%` if you need to pass raw content (PowerShell specific).

Enqueue a job (example):

```powershell
python main.py enqueue --command "python -c 'print(\"hello\")'" --max-retries 2
# or using a script file
python main.py enqueue --command "python my_script.py" --max-retries 2
```

List pending jobs:

```powershell
python main.py list --state pending
```

Start workers in foreground (1 worker):

```powershell
python main.py worker-start --count 1
```

Start workers in the background (detached) and write a pidfile `queuectl.pid`:

```powershell
python main.py worker-start --background --count 1
```

Stop background workers (reads `queuectl.pid`):

```powershell
python main.py worker-stop
```

Dead Letter Queue (DLQ):

```powershell
python main.py dlq list
python main.py dlq retry <job_id>
```

Show job state counts:

```powershell
python main.py status
```

### New enqueue options

These options were added to improve usability for complex commands and scheduling:

- `--command-file PATH` — read the command string from a file (avoids shell quoting).
- `--job-file PATH` — read a full job JSON payload (useful to supply id, command, max_retries, and scheduling fields).
- `--delay SECONDS` — schedule the job to run after the given delay (seconds).
- `--run-at TIMESTAMP` — schedule the job to run at an ISO UTC timestamp (e.g. 2025-11-09T12:00:00Z).

Examples (PowerShell):

```powershell
# 1) Command file: write the command into a file and enqueue it
Set-Content -Path cmd.txt -Value 'python -c "import sys; sys.exit(0)"'
python main.py enqueue --command-file cmd.txt --max-retries 2

# 2) Full job JSON from a file
Set-Content -Path job.json -Value '{"id":"job-file-1","command":"python -c \"print(1)\"","max_retries":1}'
python main.py enqueue --job-file job.json

# 3) Schedule a command to run after 5 seconds
python main.py enqueue --command "python my_script.py" --delay 5

# 4) Schedule a job to run at a specific UTC timestamp
python main.py enqueue --command "python my_script.py" --run-at 2025-11-09T12:00:00Z
```

---

## Job model

Each job is stored with at least the following fields:

```json
{
  "id": "<uuid>",
  "command": "python my_script.py",
  "state": "pending|processing|completed|failed|dead",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2025-11-04T10:30:00Z",
  "updated_at": "2025-11-04T10:30:00Z",
  "next_run_at": null
}
```

Jobs that fail are retried with exponential backoff:

```
delay_seconds = backoff_base ** attempts
```

After `attempts > max_retries` the job is moved to state `dead` and will appear in the DLQ.

---

## Design & architecture (short)

- Persistence: SQLite (`queuectl.db`) with WAL mode enabled for better concurrency.
- Claim model: workers perform an atomic `BEGIN IMMEDIATE` + `SELECT ... LIMIT 1` and `UPDATE` to mark `processing` to avoid duplicate processing.
- Workers: `worker.py` provides both threaded workers and the ability to spawn process-based workers (`--use-processes`).
- Command execution: `subprocess.run(..., shell=True)` with `capture_output` and optional `timeout` from config.
- Logs: workers append command stdout/stderr to `job_logs/<job_id>.log`.

Security note: commands are executed with `shell=True` and may be unsafe in untrusted environments. If you plan to run untrusted payloads, sandboxing is required.

---

## Testing

This project includes pytest tests under `tests/`.

Run the full test suite (recommended inside a virtualenv):

```powershell
pip install -r requirements.txt
python -m pytest -q
```

The tests cover:
- Basic job completion
- Failed job retry and DLQ movement
- Multi-worker concurrency (no duplicate processing)
- CLI integration tests that exercise enqueue, DLQ retry, and timeout behavior

If you run tests on Windows PowerShell, the README examples above show how to quote commands safely.

---

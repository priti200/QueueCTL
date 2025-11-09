
# QueueCTL

A minimal, CLI-first background job queue implemented in Python. QueueCTL supports enqueueing shell commands, running multiple workers, retry/backoff, persistent storage (SQLite), a Dead Letter Queue (DLQ), and a small set of CLI utilities for administration and inspection.

---

## Summary

- Atomic job claiming to prevent double processing
- Multiple worker modes (threaded or process-backed)
- Exponential backoff retries and DLQ for permanently failed jobs
- Scheduling (delay / run-at) and per-job timeouts
- SQLite persistence with optional DB path override via `QUEUECTL_DB_PATH`

Key files:
- `main.py` - CLI entrypoint
- `worker.py` - worker loop and runner
- `job_storage.py` - SQLite persistence and claim logic
- `dead_letter_queue.py` - thin DLQ helpers
- `config.py` - JSON-backed configuration

---

## Project structure

```
queuectl/
├── main.py
├── worker.py
├── job_storage.py
├── dead_letter_queue.py
├── config.py
├── Dockerfile
├── .gitignore
└── tests/
```

---

## Requirements & install

- Python 3.8+ (tested on 3.10)
- pip

Install dependencies:

```powershell
pip install -r requirements.txt
```

Initialize the database (creates a sqlite DB at the current working directory by default):

```powershell
python main.py init
```

To use a custom DB path, set the `QUEUECTL_DB_PATH` environment variable before running commands:

```powershell
$env:QUEUECTL_DB_PATH = "C:\path\to\queuectl.db"
python main.py init
```

---

## Quick start

1. Enqueue a simple job:

```powershell
python main.py enqueue --id demo-1 --command-file cmd.txt
```

2. Start a worker in the foreground (1 worker):

```powershell
python main.py worker-run --count 1
```

3. Check status and lists:

```powershell
python main.py status
python main.py list --state pending
```

4. View job logs (PowerShell):

```powershell
Get-Content .\job_logs\demo-1.log -Tail 200
```

---

## CLI reference

Enqueue:

- `--id` - job id
- `--command` - shell command to run
- `--command-file` - read command string from a file (recommended on PowerShell)
- `--job-file` - read a full job JSON payload from a file
- `--max-retries` - override default max retries
- `--delay` - schedule job to run after N seconds
- `--run-at` - schedule job at ISO-8601 UTC timestamp

Examples:

PowerShell one-liner (use a command file to avoid quoting issues):

```powershell
Set-Content -Path cmd.txt -Value "python -c 'print(\"hello from job\")'"
python main.py enqueue --id demo-1 --command-file cmd.txt
```

cmd.exe single-line example:

```cmd
python main.py enqueue --id demo-1 --command "python -c \"print('hello from job')\""
```

Worker management:

- `python main.py worker-run --count N` - run N workers in foreground
- `python main.py worker-start --count N` - (if supported) start background workers
- `python main.py worker-stop` - stop background workers

DLQ:

- `python main.py dlq list`
- `python main.py dlq retry <job_id>`

Config:

- `python main.py config set <key> <value>`
- `python main.py config get <key>`

Status & listing:

- `python main.py status` - show counts by state
- `python main.py list --state pending|processing|completed|dead`

---

## Job model

Jobs are stored with these primary fields:

```json
{
  "id": "<id>",
  "command": "...",
  "state": "pending|processing|completed|dead",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "...",
  "updated_at": "...",
  "next_run_at": null
}
```

Retries use exponential backoff: `delay_seconds = backoff_base ** attempts`.

---

## Design & architecture

- Persistence: SQLite (WAL mode enabled for better concurrency)
- Claiming: atomic `BEGIN IMMEDIATE` + `SELECT ... LIMIT 1` + `UPDATE` to mark processing
- Workers: thread-based workers; `worker.py` supports running multiple workers and an optional process-based mode
- Execution: `subprocess.run(..., shell=True)` with `capture_output` and optional timeout from configuration
- Logs: per-job append-only logs under `job_logs/` (stdout/stderr captured)

Security note: commands are executed using the shell. Do not enqueue untrusted commands without sandboxing.

---

## Testing

Run tests with pytest:

```powershell
pip install -r requirements.txt
python -m pytest -q
```

Tests include smoke and integration tests that cover job success, retries and DLQ behavior, file-based enqueue, and scheduling.

---

## Configuration

Default keys (configured via `queuectl_config.json`):

- `max_retries` (default 3)
- `backoff_base` (default 2)
- `job_timeout` (seconds, default 0 → no timeout)

Use the CLI to get/set configuration values.

---

## Advanced usage

- Docker: a `Dockerfile` is provided. Build the image and mount a host folder at `/data` to persist the SQLite DB:

```powershell
docker build -t queuectl:local .
docker run --rm -v %cd%\queuectl-data:/data queuectl:local status
```

- Asciinema / terminal recordings are recommended for short demos; for full screencasts use OBS and a short narrated script.

---

## Troubleshooting

- If job logs are missing, confirm you are running the worker from the repository root so `./job_logs` is created in the expected location.
- If Click complains about unexpected extra arguments on Windows PowerShell, prefer `--command-file` or build the command string in a variable and pass it as a single argument.
- For SQLite locked errors, increase busy timeout or ensure no long-running exclusive transactions are active.

---

## Performance considerations

- Start worker count near your CPU core count and adjust based on workload.
- SQLite is suitable for moderate throughput; for heavy workloads consider an alternative datastore.

---

## Security

- Commands run in shell context: validate inputs and avoid enqueueing untrusted payloads.
- Protect the DB file and log files with appropriate filesystem permissions.

---

## Acknowledgments

- Built as a small, pragmatic job queue with Python and SQLite.

---

## Demo / Resources

See `README.md` examples above for quick commands and the `tests/` folder for automated checks.
You can view the demonstration of **QueueCTL** here:  
[🔗 QueueCTL Demo Video](https://drive.google.com/file/d/17udLc5bh2u8yeUSoG1CAEiG5nlu1qRfV/view?usp=sharing)
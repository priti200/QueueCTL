FROM python:3.10-slim

# Minimal image for QueueCTL
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install system deps needed for SQLite and common packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements if present and install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt || true

# Copy application
COPY . /app

# Default DB path inside container (bind-mount /data to persist)
ENV QUEUECTL_DB_PATH=/data/queuectl.db
VOLUME ["/data"]

# Make job_logs writable
RUN mkdir -p /app/job_logs && chmod a+rwx /app/job_logs

# Default: run a single threaded worker. Override CMD or pass different args.
ENTRYPOINT ["python", "-u", "main.py"]
CMD ["worker-run", "--count", "1"]

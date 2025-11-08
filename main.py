import click
import json
from job_storage import init_db, add_job, current_time

@click.group()
def cli():
    """queuectl - Job Queue CLI"""
    pass

@cli.command()
@click.argument('job_json')
def enqueue(job_json):
    """Add a new job to the queue."""
    try:
        job = json.loads(job_json)
        # Set defaults/derive fields
        now = current_time()
        job_data = {
            'id': job['id'],
            'command': job['command'],
            'state': 'pending',
            'attempts': 0,
            'max_retries': job.get('max_retries', 3),
            'created_at': now,
            'updated_at': now
        }
        add_job(job_data)
        click.echo(f"Enqueued job: {job_data['id']}")
    except Exception as e:
        click.echo(f"Failed to enqueue job: {e}", err=True)

@cli.command()
def init():
    """Initialize database (run on first setup)."""
    init_db()
    click.echo("Database initialized.")

if __name__ == "__main__":
    cli()

import click
from job_storage import init_db

@click.group()
def cli():
    """queuectl - Job Queue CLI"""
    pass

@cli.command()
def init():
    """Initialize database (run on first setup)."""
    init_db()
    click.echo("Database initialized.")

if __name__ == "__main__":
    cli()

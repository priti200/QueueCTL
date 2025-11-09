import job_storage as store


def list_dead():
    return store.list_jobs_by_state('dead')


def retry(job_id):
    # returns True if retried
    # job_storage.retry_dead_job will only update if state='dead'
    try:
        store.retry_dead_job(job_id)
        return True
    except Exception:
        return False

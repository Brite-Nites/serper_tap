"""Prefect tasks for the scraping pipeline."""

from src.tasks.bigquery_tasks import (
    create_job_task,
    dequeue_batch_task,
    enqueue_queries_task,
    get_job_status_task,
    get_running_jobs_task,
    get_zips_for_state_task,
    mark_job_done_task,
    skip_remaining_pages_task,
    store_places_task,
    update_job_stats_task,
    update_query_status_task,
)
from src.tasks.serper_tasks import fetch_serper_place_task

__all__ = [
    # BigQuery tasks
    "create_job_task",
    "get_zips_for_state_task",
    "enqueue_queries_task",
    "dequeue_batch_task",
    "store_places_task",
    "update_query_status_task",
    "update_job_stats_task",
    "skip_remaining_pages_task",
    "get_job_status_task",
    "get_running_jobs_task",
    "mark_job_done_task",
    # Serper tasks
    "fetch_serper_place_task",
]

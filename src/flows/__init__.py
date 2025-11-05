"""Prefect flows for the scraping pipeline."""

from src.flows.create_job import create_scraping_job
from src.flows.process_batches import process_job_batches

__all__ = [
    "create_scraping_job",
    "process_job_batches",
]

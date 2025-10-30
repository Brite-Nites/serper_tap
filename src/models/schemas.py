"""Pydantic models for type-safe data validation and serialization.

These models represent the core data structures used throughout the pipeline.
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class JobParams(BaseModel):
    """Input parameters for creating a scraping job.

    These parameters are validated when a new job is created.
    """

    keyword: str = Field(
        ...,
        min_length=1,
        description="Search keyword (e.g., 'bars', 'restaurants')"
    )
    state: str = Field(
        ...,
        min_length=2,
        max_length=2,
        description="Two-letter US state code (e.g., 'AZ', 'CA')"
    )
    pages: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Number of pages to scrape per zip code (1-10)"
    )
    batch_size: int = Field(
        default=100,
        ge=1,
        le=500,
        description="Number of queries to process per batch"
    )
    concurrency: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Concurrency level for parallel processing"
    )
    dry_run: bool = Field(
        default=False,
        description="If True, simulate job without making API calls"
    )

    @field_validator("state")
    @classmethod
    def state_must_be_uppercase(cls, v: str) -> str:
        """Ensure state code is uppercase."""
        return v.upper()


class JobStats(BaseModel):
    """Rollup statistics for a scraping job.

    These totals are calculated from the serper_queries and serper_places tables.
    """

    zips: int = Field(default=0, description="Number of unique zip codes")
    queries: int = Field(default=0, description="Total queries created")
    successes: int = Field(default=0, description="Successful API calls")
    failures: int = Field(default=0, description="Failed API calls")
    places: int = Field(default=0, description="Total places scraped")
    credits: int = Field(default=0, description="Serper API credits consumed")


class JobRecord(BaseModel):
    """Represents a row in the serper_jobs table."""

    job_id: str
    keyword: str
    state: str
    pages: int
    dry_run: bool
    concurrency: int
    status: Literal["running", "done", "failed"]
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    totals: JobStats = Field(default_factory=JobStats)


class QueryRecord(BaseModel):
    """Represents a row in the serper_queries table."""

    job_id: str
    zip: str
    page: int
    q: str = Field(description="Query text sent to Serper (e.g., '85001 bars')")
    status: Literal["queued", "processing", "success", "failed", "skipped"] = "queued"
    claim_id: str | None = None
    claimed_at: datetime | None = None
    api_status: int | None = None
    results_count: int | None = None
    credits: int | None = None
    error: str | None = None
    ran_at: datetime | None = None


class PlaceRecord(BaseModel):
    """Represents a row in the serper_places table."""

    ingest_id: str
    job_id: str
    source: str = "serper_places"
    source_version: str = "v1"
    ingest_ts: datetime
    keyword: str
    state: str
    zip: str
    page: int
    place_uid: str = Field(description="placeId or cid from Serper API")
    payload: dict[str, Any] = Field(description="Full Serper response for this place")
    api_status: int | None = None
    api_ms: int | None = None
    results_count: int | None = None
    credits: int | None = None
    error: str | None = None


class ZipCodeRecord(BaseModel):
    """Represents a zip code from reference.geo_zip_all table."""

    zip: str
    state: str
    # Additional fields may exist in the reference table but are not required

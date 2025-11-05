"""Configuration management for Serper scraping pipeline.

Loads and validates environment variables using Pydantic Settings.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    Environment variables are loaded from .env file or system environment.
    All fields are validated at application startup.
    """

    # Google Cloud BigQuery Configuration
    google_application_credentials: str | None = Field(
        default=None,
        description="Path to Google Cloud service account JSON key file (optional if using ADC)"
    )
    bigquery_project_id: str = Field(
        ...,
        description="GCP project ID containing BigQuery datasets"
    )
    bigquery_dataset: str = Field(
        default="raw_data",
        description="BigQuery dataset name for serper tables"
    )

    # Serper API Configuration
    serper_api_key: str = Field(
        default="",
        description="API key for Serper.dev"
    )
    use_mock_api: bool = Field(
        default=True,
        description="Use mock API (True) or real Serper API (False)"
    )

    # Prefect Configuration (will be used in Phase 2)
    prefect_api_url: str = Field(
        default="http://localhost:4200/api",
        description="Prefect API URL for orchestration"
    )
    prefect_deployment_name: str = Field(
        default="process-job-batches/production",
        description="Fully-qualified deployment name for run_deployment()"
    )

    # Application Configuration
    default_batch_size: int = Field(
        default=100,
        description="Default number of queries to process per batch"
    )
    default_concurrency: int = Field(
        default=20,
        description="Default concurrency level for parallel processing"
    )
    default_pages: int = Field(
        default=3,
        description="Default number of pages to scrape per zip code"
    )

    # Processor Configuration
    processor_max_workers: int = Field(
        default=100,
        description="Maximum concurrent workers for batch processor (Prefect ConcurrentTaskRunner)"
    )
    processor_loop_delay_seconds: float = Field(
        default=1.0,
        description="Delay in seconds between batch processing iterations"
    )

    # API Configuration
    serper_timeout_seconds: float = Field(
        default=30.0,
        description="HTTP timeout for Serper API calls"
    )
    serper_retries: int = Field(
        default=3,
        description="Number of retries for failed API calls"
    )
    serper_retry_delay_seconds: int = Field(
        default=5,
        description="Initial delay between retries (exponential backoff applied)"
    )
    serper_max_backoff_seconds: int = Field(
        default=60,
        description="Maximum backoff delay between retries"
    )

    # Optimization Settings
    early_exit_threshold: int = Field(
        default=10,
        description="Skip pages 2-3 if page 1 has fewer than this many results"
    )

    # Cost Management
    daily_budget_usd: float = Field(
        default=100.0,
        description="Daily budget limit in USD"
    )
    budget_soft_threshold_pct: int = Field(
        default=80,
        description="Warn when daily budget reaches this percentage"
    )
    budget_hard_threshold_pct: int = Field(
        default=100,
        description="Block new jobs when daily budget reaches this percentage"
    )
    cost_per_credit: float = Field(
        default=0.01,
        description="Cost per API credit in USD"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


# Global settings instance - loaded once at import time
settings = Settings()

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
    google_application_credentials: str = Field(
        ...,
        description="Path to Google Cloud service account JSON key file"
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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


# Global settings instance - loaded once at import time
settings = Settings()

"""Serper API integration tasks.

Supports both mock and real Serper API implementations via environment variable toggle.
"""

import random
import time
from typing import Any

import httpx
from prefect import task, get_run_logger

from src.utils.config import settings
from src.utils.timing import timing


@task(
    retries=settings.serper_retries,
    retry_delay_seconds=settings.serper_retry_delay_seconds,
    retry_jitter_factor=0.5  # Add jitter to prevent thundering herd
)
def fetch_serper_place_task(query: dict[str, Any]) -> dict[str, Any]:
    """Fetch place data from Serper API (mock or real based on settings).

    Uses mock API by default (settings.use_mock_api=True) for testing without
    spending credits. Set use_mock_api=False to use real Serper.dev API.

    Args:
        query: Dict with keys:
            - zip: Zip code string
            - page: Page number (1-3)
            - q: Query text (e.g., "85001 bars")

    Returns:
        Dict matching Serper API response structure:
        {
            "places": [
                {
                    "position": 1,
                    "title": "Business Name",
                    "placeId": "ChIJ...",
                    "address": "123 Main St",
                    ...
                }
            ],
            "credits": 1,
            "searchParameters": {
                "q": "85001 bars",
                "page": 1
            }
        }
    """
    logger = get_run_logger()

    # Toggle between mock and real API
    if settings.use_mock_api:
        logger.debug(f"Using MOCK API for query: {query['q']} page {query['page']}")
        return _fetch_mock_api(query)
    else:
        logger.info(f"Using REAL Serper API for query: {query['q']} page {query['page']}")
        return _fetch_real_api(query)


def _fetch_mock_api(query: dict[str, Any]) -> dict[str, Any]:
    """Mock Serper API implementation for testing without spending credits.

    Returns realistic data with randomized results (0-10 places) to test
    both normal processing and early exit optimization.
    """
    # Simulate network latency
    time.sleep(random.uniform(0.1, 0.3))

    # Randomize results to test different scenarios
    # - 0 results: No places found
    # - 1-9 results: Sparse area, should trigger early exit on page 1
    # - 10 results: Dense area, all pages should be processed
    num_results = random.randint(0, 10)

    places = []
    for i in range(num_results):
        # Generate mock place data
        place_position = i + 1
        place_id = f"mock-{query['zip']}-p{query['page']}-{i:02d}"

        place = {
            "position": place_position,
            "title": f"Mock Business {place_position} in {query['zip']}",
            "placeId": place_id,
            "address": f"{place_position}00 Main St, Zip {query['zip']}",
            "latitude": round(33.4484 + random.uniform(-0.1, 0.1), 6),  # Arizona-ish coords
            "longitude": round(-112.0740 + random.uniform(-0.1, 0.1), 6),
            "rating": round(random.uniform(3.0, 5.0), 1),
            "ratingCount": random.randint(10, 500),
            "category": "Bar",
            "phoneNumber": f"+1 480-555-{random.randint(1000, 9999)}",
            "website": f"https://mockbusiness{place_position}.example.com",
            "cid": f"{random.randint(10**15, 10**16-1)}",  # Some places use cid instead of placeId
        }

        places.append(place)

    # Build response matching Serper API structure
    response = {
        "places": places,
        "credits": 1,  # Serper charges 1 credit per request
        "searchParameters": {
            "q": query["q"],
            "page": query["page"],
            "num": 10  # Always request 10 results per page
        }
    }

    return response


def _fetch_real_api(query: dict[str, Any]) -> dict[str, Any]:
    """Real Serper API implementation with structured error handling.

    Makes actual API call to Serper.dev and returns real place data.
    Requires settings.serper_api_key to be configured.

    Raises:
        ValueError: If serper_api_key is not configured
        httpx.HTTPStatusError: If API returns error status
        httpx.TimeoutException: If API call times out
        httpx.RequestError: For network errors
    """
    logger = get_run_logger()

    if not settings.serper_api_key:
        raise ValueError(
            "SERPER_API_KEY environment variable is required when use_mock_api=False. "
            "Get your API key from https://serper.dev"
        )

    # Make API request
    try:
        with timing(f"Serper API call: {query['q']} page {query['page']}"):
            response = httpx.post(
                "https://google.serper.dev/places",
                headers={
                    "X-API-KEY": settings.serper_api_key,
                    "Content-Type": "application/json"
                },
                json={
                    "q": query["q"],
                    "page": query["page"],
                    "num": 10  # Request 10 results per page
                },
                timeout=settings.serper_timeout_seconds
            )
            response.raise_for_status()

        # Successful response
        result = response.json()
        logger.debug(
            f"Serper API success: {query['q']} page {query['page']} "
            f"- {len(result.get('places', []))} places, {result.get('credits', 1)} credits"
        )
        return result

    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code

        # Categorize errors by status code
        if status_code == 401:
            logger.error(f"Serper API authentication failed - check API key")
        elif status_code == 429:
            logger.warning(f"Serper API rate limit exceeded - will retry")
        elif 400 <= status_code < 500:
            logger.error(
                f"Serper API client error {status_code} for {query['q']} page {query['page']}: "
                f"{e.response.text}"
            )
        else:  # 500+
            logger.error(
                f"Serper API server error {status_code} for {query['q']} page {query['page']}: "
                f"{e.response.text}"
            )

        # Re-raise for Prefect retry logic
        # Prefect will retry automatically with exponential backoff
        raise

    except httpx.TimeoutException as e:
        logger.warning(
            f"Serper API timeout after {settings.serper_timeout_seconds}s "
            f"for {query['q']} page {query['page']} - will retry"
        )
        raise

    except httpx.RequestError as e:
        # Network errors (DNS, connection refused, etc.)
        logger.error(
            f"Serper API network error for {query['q']} page {query['page']}: "
            f"{type(e).__name__}: {e}"
        )
        raise

    except Exception as e:
        # Catch-all for unexpected errors
        logger.error(
            f"Unexpected error calling Serper API for {query['q']} page {query['page']}: "
            f"{type(e).__name__}: {e}"
        )
        raise

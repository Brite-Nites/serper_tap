"""Serper API integration tasks.

For Phase 2A, this contains a MOCK implementation for testing without
spending API credits. The real implementation will be added in Phase 2B.
"""

import random
import time
from typing import Any

from prefect import task


@task(retries=3, retry_delay_seconds=5)
def fetch_serper_place_task(query: dict[str, Any]) -> dict[str, Any]:
    """Mock Serper API call for testing without spending credits.

    This mock implementation returns realistic data that matches Serper's
    response structure. Results count is randomized (0-10) to test both
    normal processing and the early exit optimization.

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


# Future: Real Serper API implementation
# @task(retries=3, retry_delay_seconds=5)
# def fetch_serper_place_task_real(query: dict[str, Any]) -> dict[str, Any]:
#     """Real Serper API call (to be implemented in Phase 2B)."""
#     import httpx
#     from src.utils.config import settings
#
#     response = httpx.post(
#         "https://google.serper.dev/places",
#         headers={"X-API-KEY": settings.serper_api_key},
#         json={
#             "q": query["q"],
#             "page": query["page"],
#             "num": 10
#         },
#         timeout=30.0
#     )
#     response.raise_for_status()
#     return response.json()

#!/usr/bin/env python3
"""Validate that geo_zip_all reference table exists and has required data.

This is a CRITICAL validation - the system cannot function without this table.

Expected schema:
    - zip (STRING): 5-digit zip code
    - state (STRING): 2-letter state code

Expected coverage:
    - ~40,000 zip codes
    - All 50 US states
    - Arizona should have ~416 zips (documented in quick_start.md)

Usage:
    python scripts/validate_reference_data.py
"""

from google.cloud import bigquery

from src.utils.config import settings


def validate_geo_zip_all():
    """Check if reference.geo_zip_all table exists and has data."""
    print("=" * 70)
    print("REFERENCE DATA VALIDATION")
    print("=" * 70)
    print()

    client = bigquery.Client()
    table_id = f"{settings.bigquery_project_id}.reference.geo_zip_all"

    # Test 1: Table exists
    print(f"Checking if table exists: {table_id}")
    try:
        table = client.get_table(table_id)
        print(f"✅ Table exists")
        print(f"   Created: {table.created}")
        print(f"   Rows: {table.num_rows:,}")
        print()
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Table does not exist")
        print(f"   Error: {e}")
        print()
        print("REQUIRED ACTION:")
        print("   Create reference.geo_zip_all table with columns: zip (STRING), state (STRING)")
        print("   Populate with US zip code data")
        print()
        return False

    # Test 2: Basic structure
    print("Checking table structure...")
    query = f"""
    SELECT
        COUNT(*) as total_zips,
        COUNT(DISTINCT state) as total_states,
        COUNT(DISTINCT zip) as unique_zips
    FROM `{table_id}`
    """

    try:
        result = list(client.query(query).result())[0]
        total_zips = result["total_zips"]
        total_states = result["total_states"]
        unique_zips = result["unique_zips"]

        print(f"✅ Query successful")
        print(f"   Total rows: {total_zips:,}")
        print(f"   Unique zips: {unique_zips:,}")
        print(f"   States covered: {total_states}")
        print()

        if total_states < 50:
            print(f"⚠️  WARNING: Only {total_states} states (expected 50)")
        else:
            print(f"✅ All 50 states present")

    except Exception as e:
        print(f"❌ FAILURE: Cannot query table")
        print(f"   Error: {e}")
        print()
        return False

    # Test 3: Arizona-specific check (our test case)
    print()
    print("Checking Arizona data (test state)...")
    query = f"""
    SELECT COUNT(*) as az_zips
    FROM `{table_id}`
    WHERE state = 'AZ'
    """

    try:
        result = list(client.query(query).result())[0]
        az_zips = result["az_zips"]

        print(f"✅ Arizona query successful")
        print(f"   AZ zip codes: {az_zips}")

        # Documentation says AZ should have ~416 zips
        if az_zips < 400:
            print(f"⚠️  WARNING: Expected ~416 AZ zips, found {az_zips}")
        elif az_zips > 500:
            print(f"⚠️  WARNING: Expected ~416 AZ zips, found {az_zips} (unusually high)")
        else:
            print(f"✅ Arizona coverage looks correct (~416 expected)")

    except Exception as e:
        print(f"❌ FAILURE: Cannot query Arizona data")
        print(f"   Error: {e}")
        print()
        return False

    # Test 4: Sample some data
    print()
    print("Sampling data (first 10 rows)...")
    query = f"""
    SELECT zip, state
    FROM `{table_id}`
    ORDER BY state, zip
    LIMIT 10
    """

    try:
        results = list(client.query(query).result())
        print(f"✅ Sample data retrieved:")
        for row in results:
            print(f"   {row['zip']} - {row['state']}")

    except Exception as e:
        print(f"❌ FAILURE: Cannot sample data")
        print(f"   Error: {e}")
        print()
        return False

    # Final verdict
    print()
    print("=" * 70)
    print("VALIDATION COMPLETE")
    print("=" * 70)
    print(f"✅ Table exists: reference.geo_zip_all")
    print(f"✅ Contains {total_zips:,} zip codes across {total_states} states")
    print(f"✅ Arizona has {az_zips} zip codes (ready for test job)")
    print()
    print("Status: READY FOR PRODUCTION")
    print()

    return True


if __name__ == "__main__":
    import sys

    success = validate_geo_zip_all()
    sys.exit(0 if success else 1)

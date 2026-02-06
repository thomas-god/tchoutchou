# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "httpx>=0.28.1",
# ]
# ///

import httpx
import sqlite3
import sys
from pathlib import Path


def create_museum_table(cursor: sqlite3.Cursor) -> None:
    """
    Create the t_museum table and associated indexes.

    Args:
        cursor: SQLite database cursor
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS t_museum (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            postal_code TEXT UNIQUE NOT NULL,
            museum_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_museum_postal_code ON t_museum(postal_code)
    """)


def fetch_museum_data() -> list[dict]:
    """
    Fetch museum count per postal code from French culture API.

    Returns:
        List of dictionaries with postal_code and count
    """
    url = "https://data.culture.gouv.fr/api/explore/v2.1/catalog/datasets/liste-et-localisation-des-musees-de-france/records"
    params = {
        "select": "count(*) as count",
        "group_by": "code_postal",
        "limit": 1000,
    }

    try:
        print("Fetching museum data from culture.gouv.fr API...")
        response = httpx.get(url, params=params, timeout=30.0)
        response.raise_for_status()
        data = response.json()

        total_count = data.get("total_count", 0)
        results = data.get("results", [])

        print(f"Fetched {len(results)} postal codes with museum data")

        # Warn if we hit the limit
        if total_count > 1000:
            print(
                f"WARNING: API returned {total_count} total records but limit is 1000. "
                f"Some postal codes may be missing!",
                file=sys.stderr,
            )

        return results
    except httpx.HTTPStatusError as e:
        print(
            f"HTTP error {e.response.status_code}: {e.response.reason_phrase}",
            file=sys.stderr,
        )
        return []
    except httpx.HTTPError as e:
        print(f"HTTP error: {str(e)}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"Error: {type(e).__name__}: {str(e)}", file=sys.stderr)
        return []


def insert_museum_data(cursor: sqlite3.Cursor, data: list[dict]) -> int:
    """
    Insert museum data into the database.

    Args:
        cursor: SQLite database cursor
        data: List of dictionaries with code_postal and count

    Returns:
        Number of records inserted
    """
    inserted_count = 0
    skipped_null = 0

    for record in data:
        postal_code = record.get("code_postal")
        count = record.get("count")

        # Skip records with null postal code
        if postal_code is None or count is None:
            if postal_code is None:
                skipped_null += 1
            continue

        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO t_museum (postal_code, museum_count)
                VALUES (?, ?)
                """,
                (postal_code, count),
            )
            inserted_count += 1
        except Exception as e:
            print(
                f"Error inserting postal code {postal_code}: {type(e).__name__}: {str(e)}",
                file=sys.stderr,
            )

    if skipped_null > 0:
        print(f"Skipped {skipped_null} records with null postal code")

    return inserted_count


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Import museum count per postal code into database"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).parent / "nodes.db",
        help="Path to SQLite database file (default: nodes.db in script directory)",
    )

    args = parser.parse_args()

    # Connect to database
    print(f"Connecting to {args.db}...")
    conn = sqlite3.connect(args.db)
    cursor = conn.cursor()

    # Create table
    print("Creating table t_museum...")
    create_museum_table(cursor)

    # Fetch data from API
    museum_data = fetch_museum_data()

    if not museum_data:
        print("No museum data fetched. Exiting.")
        conn.close()
        return

    # Insert data
    print("Inserting museum data into database...")
    inserted = insert_museum_data(cursor, museum_data)

    # Commit and close
    conn.commit()
    conn.close()

    print(f"\n✓ Inserted {inserted} postal code records with museum counts")
    print(f"✓ Database saved to {args.db}")


if __name__ == "__main__":
    main()

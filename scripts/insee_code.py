# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "httpx>=0.28.1",
# ]
# ///

import httpx
import json
import sqlite3
import sys
import time
from pathlib import Path


def get_city_from_coordinates(lat: float, lon: float) -> dict | tuple[None, str]:
    """
    Get city information from latitude/longitude coordinates using the French API.

    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate

    Returns:
        Dictionary containing city information, or (None, error_message) if failed
    """
    url = "https://geo.api.gouv.fr/communes"
    params = {
        "lat": lat,
        "lon": lon,
        "fields": "nom,code,codeDepartement,codeRegion,population,codesPostaux",
    }

    try:
        response = httpx.get(url, params=params)
        response.raise_for_status()
        communes = response.json()

        if communes and len(communes) > 0:
            return communes[0]
        return None, "API returned empty result"
    except httpx.HTTPStatusError as e:
        error_msg = f"HTTP {e.response.status_code}: {e.response.reason_phrase}"
        print(f"HTTP error occurred: {error_msg}", file=sys.stderr)
        return None, error_msg
    except httpx.HTTPError as e:
        error_msg = f"HTTP error: {str(e)}"
        print(f"HTTP error occurred: {error_msg}", file=sys.stderr)
        return None, error_msg
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        print(f"An error occurred: {error_msg}", file=sys.stderr)
        return None, error_msg


def create_insee_table(cursor: sqlite3.Cursor) -> None:
    """
    Create the t_insee table and associated indexes.

    Args:
        cursor: SQLite database cursor
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS t_insee (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id INTEGER NOT NULL,
            insee_code TEXT,
            city_name TEXT,
            department_code TEXT,
            region_code TEXT,
            population INTEGER,
            postal_codes TEXT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (node_id) REFERENCES t_nodes(id)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_insee_node_id ON t_insee(node_id)
    """)


def enrich_cities_from_db(db_path: Path) -> None:
    """
    Load nodes from t_nodes table, enrich each with API data, and save to t_insee table.

    Args:
        db_path: Path to SQLite database file
    """
    # Connect to database
    print(f"Connecting to {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create t_insee table
    print("Creating table t_insee...")
    create_insee_table(cursor)

    # Load nodes from t_nodes
    print("Loading nodes from t_nodes...")
    cursor.execute("SELECT id, sncf_id, name, lat, lon FROM t_nodes")
    nodes = cursor.fetchall()

    total_entries = len(nodes)
    enriched_count = 0
    error_count = 0

    # Rate limiting: 50 calls/s = 0.02s per call
    rate_limit_delay = 0.02

    print(f"Processing {total_entries} nodes...")
    print(f"Rate limit: 50 calls/s (waiting {rate_limit_delay}s between calls)")

    for i, (node_id, sncf_id, name, lat, lon) in enumerate(nodes):
        try:
            # Get city information from API
            result = get_city_from_coordinates(lat, lon)

            if isinstance(result, dict):
                # Insert enriched data
                postal_codes_json = json.dumps(result.get("codesPostaux", []))
                cursor.execute(
                    """
                    INSERT INTO t_insee 
                    (node_id, insee_code, city_name, department_code, region_code, population, postal_codes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        node_id,
                        result.get("code"),
                        result.get("nom"),
                        result.get("codeDepartement"),
                        result.get("codeRegion"),
                        result.get("population"),
                        postal_codes_json,
                    ),
                )
                enriched_count += 1
            else:
                # result is (None, error_message)
                _, error_message = result
                cursor.execute(
                    """
                    INSERT INTO t_insee (node_id, error_message)
                    VALUES (?, ?)
                    """,
                    (node_id, error_message),
                )
                error_count += 1

            # Commit every 50 entries
            if (i + 1) % 50 == 0:
                conn.commit()
                print(
                    f"Progress: {i + 1}/{total_entries} | Enriched: {enriched_count} | Errors: {error_count}"
                )

            # Rate limiting
            time.sleep(rate_limit_delay)

        except Exception as e:
            error_message = f"{type(e).__name__}: {str(e)}"
            print(
                f"Error processing node {node_id} ({name}): {error_message}",
                file=sys.stderr,
            )
            cursor.execute(
                """
                INSERT INTO t_insee (node_id, error_message)
                VALUES (?, ?)
                """,
                (node_id, error_message),
            )
            error_count += 1

    # Final commit
    conn.commit()
    conn.close()

    print(f"\nDone!")
    print(f"  Total nodes: {total_entries}")
    print(f"  Enriched: {enriched_count}")
    print(f"  Errors: {error_count}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Enrich node data with INSEE codes from geo.api.gouv.fr"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).parent / "nodes.db",
        help="Path to SQLite database file (default: nodes.db in script directory)",
    )

    args = parser.parse_args()

    if not args.db.exists():
        print(f"Error: Database file {args.db} does not exist", file=sys.stderr)
        sys.exit(1)

    enrich_cities_from_db(args.db)


if __name__ == "__main__":
    main()

# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "httpx>=0.28.1",
#     "python-dotenv>=1.0.0",
# ]
# ///

import httpx
import os
import sqlite3
import sys
import time
from pathlib import Path

from dotenv import load_dotenv


def create_weather_station_table(cursor: sqlite3.Cursor) -> None:
    """
    Create the t_weather_station table and associated indexes.

    Args:
        cursor: SQLite database cursor
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS t_weather_station (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT UNIQUE NOT NULL,
            nom TEXT NOT NULL,
            department_code TEXT NOT NULL,
            poste_ouvert BOOLEAN NOT NULL,
            type_poste INTEGER NOT NULL,
            lon REAL NOT NULL,
            lat REAL NOT NULL,
            alt INTEGER NOT NULL,
            poste_public BOOLEAN NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_station_id ON t_weather_station(station_id)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_weather_station_coords ON t_weather_station(lat, lon)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_weather_station_dept ON t_weather_station(department_code)
    """)


def fetch_weather_stations(api_key: str, department_ids: list[str]) -> list[dict]:
    """
    Fetch weather stations from Meteo France API for given departments.

    Args:
        api_key: API key
        department_ids: List of department IDs to fetch stations for

    Returns:
        List of station dictionaries
    """
    url = "https://public-api.meteofrance.fr/public/DPClim/v1/liste-stations/horaire"
    headers = {"accept": "*/*", "apikey": api_key}

    all_stations = []

    for dept_id in department_ids:
        print(f"Fetching stations for department {dept_id}...")
        params = {
            "id-departement": dept_id,
            "parametre": ["temperature", "precipitation", "insolation"],
        }

        max_retries = 5
        retry_delay = 1  # Start with 1 second

        for attempt in range(max_retries):
            try:
                response = httpx.get(url, headers=headers, params=params, timeout=30.0)
                response.raise_for_status()
                stations = response.json()
                print(f"  Found {len(stations)} stations")
                # Add department code to each station
                for station in stations:
                    station["department_code"] = dept_id
                all_stations.extend(stations)
                break  # Success, exit retry loop
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2**attempt)  # Exponential backoff
                        print(
                            f"  Rate limited (429). Waiting {wait_time}s before retry {attempt + 2}/{max_retries}...",
                            file=sys.stderr,
                        )
                        time.sleep(wait_time)
                    else:
                        print(
                            f"  HTTP error 429 after {max_retries} retries: {e.response.reason_phrase}",
                            file=sys.stderr,
                        )
                else:
                    print(
                        f"  HTTP error {e.response.status_code}: {e.response.reason_phrase}",
                        file=sys.stderr,
                    )
                    break  # Non-429 error, don't retry
            except httpx.HTTPError as e:
                print(f"  HTTP error: {str(e)}", file=sys.stderr)
                break
            except Exception as e:
                print(f"  Error: {type(e).__name__}: {str(e)}", file=sys.stderr)
                break

    return all_stations


def insert_weather_stations(cursor: sqlite3.Cursor, stations: list[dict]) -> int:
    """
    Insert weather stations into the database.

    Args:
        cursor: SQLite database cursor
        stations: List of station dictionaries

    Returns:
        Number of stations inserted
    """
    inserted_count = 0

    for station in stations:
        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO t_weather_station
                (station_id, nom, department_code, poste_ouvert, type_poste, lon, lat, alt, poste_public)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    station["id"],
                    station["nom"],
                    station["department_code"],
                    station["posteOuvert"],
                    station["typePoste"],
                    station["lon"],
                    station["lat"],
                    station["alt"],
                    station["postePublic"],
                ),
            )
            inserted_count += 1
        except Exception as e:
            print(
                f"Error inserting station {station.get('id')}: {type(e).__name__}: {str(e)}",
                file=sys.stderr,
            )

    return inserted_count


def get_department_ids(cursor: sqlite3.Cursor) -> list[str]:
    """
    Get unique department codes from t_insee table.

    Args:
        cursor: SQLite database cursor

    Returns:
        List of unique department codes
    """
    cursor.execute("""
        SELECT DISTINCT department_code
        FROM t_insee
        WHERE department_code IS NOT NULL
        ORDER BY department_code
    """)
    return [row[0] for row in cursor.fetchall()]


def main() -> None:
    import argparse

    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Import weather stations from Meteo France API into database"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).parent / "nodes.db",
        help="Path to SQLite database file (default: nodes.db in script directory)",
    )
    parser.add_argument(
        "--departments",
        type=str,
        nargs="+",
        help="Department IDs to fetch (e.g., 13 75 69). If not provided, will use departments from t_insee table.",
    )

    args = parser.parse_args()

    # Get API token from argument or environment variable
    api_token = os.getenv("METEO_FRANCE_API_KEY")
    if not api_token:
        print(
            "Error: API token not provided. Set METEO_FRANCE_API_KEY in .env file",
            file=sys.stderr,
        )
        sys.exit(1)

    # Connect to database
    print(f"Connecting to {args.db}...")
    conn = sqlite3.connect(args.db)
    cursor = conn.cursor()

    # Create table
    print("Creating table t_weather_station...")
    create_weather_station_table(cursor)

    # Get department IDs
    if args.departments:
        department_ids = args.departments
        print(f"Using provided departments: {', '.join(department_ids)}")
    else:
        print("No departments provided, fetching from t_insee table...")
        department_ids = get_department_ids(cursor)
        if not department_ids:
            print("No departments found in t_insee table. Exiting.")
            conn.close()
            return
        print(f"Found {len(department_ids)} departments: {', '.join(department_ids)}")

    # Fetch stations from API
    print(f"\nFetching stations for {len(department_ids)} departments...")
    stations = fetch_weather_stations(api_token, department_ids)

    if not stations:
        print("No stations fetched. Exiting.")
        conn.close()
        return

    print(f"\nTotal stations fetched: {len(stations)}")

    # Insert stations
    print("Inserting stations into database...")
    inserted = insert_weather_stations(cursor, stations)

    # Commit and close
    conn.commit()
    conn.close()

    print(f"\n✓ Inserted {inserted} weather stations")
    print(f"✓ Database saved to {args.db}")


if __name__ == "__main__":
    main()

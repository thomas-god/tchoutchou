# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "httpx>=0.28.1",
#     "python-dotenv>=1.0.0",
# ]
# ///

import csv
import httpx
import io
import math
import os
import sqlite3
import sys
import time
from pathlib import Path

from dotenv import load_dotenv


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance between two points on Earth in kilometers.

    Args:
        lat1, lon1: Latitude and longitude of first point in decimal degrees
        lat2, lon2: Latitude and longitude of second point in decimal degrees

    Returns:
        Distance in kilometers
    """
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    # Haversine formula
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))

    # Earth's radius in kilometers
    earth_radius = 6371.0

    return earth_radius * c


def find_closest_weather_stations(db_path: Path) -> dict[int, tuple[int, str, float]]:
    """
    For each node with known department code, find the closest weather station.

    Args:
        db_path: Path to SQLite database file

    Returns:
        Dictionary mapping node_id to (weather_station_id, distance_km)
    """
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get nodes with department codes
    cursor.execute("""
        SELECT DISTINCT n.id, n.sncf_id, n.name, n.lat, n.lon, i.department_code
        FROM t_nodes n
        JOIN t_insee i ON n.id = i.node_id
        WHERE i.department_code IS NOT NULL
    """)
    nodes = cursor.fetchall()

    # Get all weather stations grouped by department
    cursor.execute("""
        SELECT id, station_id, nom, department_code, lat, lon
        FROM t_weather_station
        WHERE poste_ouvert IS TRUE;
    """)
    all_stations = cursor.fetchall()

    # Group stations by department for faster lookup
    stations_by_dept = {}
    for station in all_stations:
        ws_id, station_id, nom, dept_code, lat, lon = station
        if dept_code not in stations_by_dept:
            stations_by_dept[dept_code] = []
        stations_by_dept[dept_code].append((ws_id, station_id, nom, lat, lon))

    # Find closest station for each node
    node_to_station = {}
    no_station_count = 0

    for node_id, sncf_id, name, node_lat, node_lon, dept_code in nodes:
        # Get stations in the same department
        dept_stations = stations_by_dept.get(dept_code, [])

        if not dept_stations:
            no_station_count += 1
            continue

        # Find closest station
        min_distance = float("inf")
        closest_station_id = None
        closest_ws_id = None
        closest_station_name = None

        for ws_id, station_id, station_name, station_lat, station_lon in dept_stations:
            distance = haversine_distance(node_lat, node_lon, station_lat, station_lon)
            if distance < min_distance:
                min_distance = distance
                closest_ws_id = ws_id
                closest_station_id = station_id
                closest_station_name = station_name

        # Store mapping
        if closest_station_id:
            node_to_station[node_id] = (
                closest_ws_id,
                closest_station_id,
                min_distance,
            )

    conn.close()

    if no_station_count > 0:
        print(
            f"Warning: {no_station_count} nodes could not be matched to a weather station",
            file=sys.stderr,
        )

    return node_to_station


def request_weather_data(
    api_key: str,
    station_id: str,
    date_start: str,
    date_end: str,
    max_retries: int = 5,
) -> str | None:
    """
    Request weather data for a station and time period. Returns command ID.

    Args:
        api_key: Meteo France API key
        station_id: Weather station ID
        date_start: Start date in format YYYY-MM-DDT00:00:00Z
        date_end: End date in format YYYY-MM-DDT00:00:00Z
        max_retries: Maximum number of retries for rate limiting

    Returns:
        Command ID string if successful, None otherwise
    """
    url = (
        "https://public-api.meteofrance.fr/public/DPClim/v1/commande-station/mensuelle"
    )
    headers = {"accept": "*/*", "apikey": api_key}
    params = {
        "id-station": station_id,
        "date-deb-periode": date_start,
        "date-fin-periode": date_end,
    }

    retry_delay = 1

    for attempt in range(max_retries):
        try:
            response = httpx.get(url, headers=headers, params=params, timeout=30.0)
            response.raise_for_status()
            data = response.json()
            command_id = data.get("elaboreProduitAvecDemandeResponse", {}).get("return")
            return command_id
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2**attempt)
                    time.sleep(wait_time)
                else:
                    print(
                        f"Rate limited requesting data for station {station_id}",
                        file=sys.stderr,
                    )
            else:
                print(
                    f"HTTP {e.response.status_code} requesting data for station {station_id}",
                    file=sys.stderr,
                )
                break
        except Exception as e:
            print(
                f"Error requesting data for station {station_id}: {type(e).__name__}: {str(e)}",
                file=sys.stderr,
            )
            break

    return None


def fetch_weather_csv(
    api_key: str, command_id: str, max_retries: int = 5
) -> str | None:
    """
    Fetch weather data CSV using command ID.

    Args:
        api_key: Meteo France API key
        command_id: Command ID from request_weather_data
        max_retries: Maximum number of retries for rate limiting

    Returns:
        CSV data as string if successful, None otherwise
    """
    url = "https://public-api.meteofrance.fr/public/DPClim/v1/commande/fichier"
    headers = {"accept": "*/*", "apikey": api_key}
    params = {"id-cmde": command_id}

    retry_delay = 1

    for attempt in range(max_retries):
        try:
            response = httpx.get(url, headers=headers, params=params, timeout=30.0)
            response.raise_for_status()
            return response.text
        except httpx.HTTPStatusError as e:
            # Either the file is not yet available (404) or we are rate-limited (429)
            if e.response.status_code in [404, 429]:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2**attempt)
                    time.sleep(wait_time)
                else:
                    print(
                        f"Rate limited fetching CSV for command {command_id}",
                        file=sys.stderr,
                    )
            else:
                print(
                    f"HTTP {e.response.status_code} fetching CSV for command {command_id}",
                    file=sys.stderr,
                )
                break
        except Exception as e:
            print(
                f"Error fetching CSV for command {command_id}: {type(e).__name__}: {str(e)}",
                file=sys.stderr,
            )
            break

    return None


def fetch_weather_data_for_nodes(
    db_path: Path,
    api_key: str,
) -> tuple[dict[int, dict[int, str]], dict[int, tuple[int, str, float]]]:
    """
    Fetch weather data for all nodes for years 2020-2025 (split into yearly requests as per API limit).

    Args:
        db_path: Path to SQLite database
        api_key: Meteo France API key

    Returns:
        Tuple of (node_to_csv_by_year, node_to_station) where:
        - node_to_csv_by_year: Dictionary mapping node_id to year to CSV data
        - node_to_station: Dictionary mapping node_id to (weather_station_id, station_id, distance_km)
    """
    # Find closest stations for all nodes
    node_to_station = find_closest_weather_stations(db_path)

    # Define years to fetch (2020-2025)
    years = [2020, 2021, 2022, 2023, 2024, 2025]

    # Fetch weather data for each unique station
    station_to_csv_by_year = {}  # station_id -> year -> csv_data
    node_to_csv_by_year = {}  # node_id -> year -> csv_data

    total_requests = len(node_to_station) * len(years)
    print(
        f"Fetching weather data for {len(node_to_station)} nodes across {len(years)} years..."
    )

    request_count = 0

    for node_id, (ws_id, station_id, distance) in node_to_station.items():
        # Initialize year dict for this node
        if node_id not in node_to_csv_by_year:
            node_to_csv_by_year[node_id] = {}

        for year in years:
            # Check if we already fetched data for this station and year
            if (
                station_id in station_to_csv_by_year
                and year in station_to_csv_by_year.get(station_id, {})
            ):
                node_to_csv_by_year[node_id][year] = station_to_csv_by_year[station_id][
                    year
                ]
                request_count += 1
                continue

            # Prepare date range for the year
            date_start = f"{year}-01-01T00:00:00Z"
            date_end = f"{year + 1}-01-01T00:00:00Z"

            # Request weather data
            command_id = request_weather_data(api_key, station_id, date_start, date_end)

            if not command_id:
                print(
                    f"Failed to request data for node {node_id}, station {station_id}, year {year}",
                    file=sys.stderr,
                )
                request_count += 1
                continue

            # Fetch CSV data
            csv_data = fetch_weather_csv(api_key, command_id)

            if csv_data:
                # Cache at station level
                if station_id not in station_to_csv_by_year:
                    station_to_csv_by_year[station_id] = {}
                station_to_csv_by_year[station_id][year] = csv_data
                node_to_csv_by_year[node_id][year] = csv_data
            else:
                print(
                    f"Failed to fetch CSV for node {node_id}, station {station_id}, year {year}",
                    file=sys.stderr,
                )

            request_count += 1

            # Progress update every 10 requests
            if request_count % 10 == 0:
                print(f"Progress: {request_count}/{total_requests} requests processed")

            # Small delay to avoid rate limiting (100 req/min theoretical limit)
            time.sleep(60 / 100)

    successful_node_years = sum(
        len(years_dict) for years_dict in node_to_csv_by_year.values()
    )
    print(f"Successfully fetched {successful_node_years} node-year combinations")

    return node_to_csv_by_year, node_to_station


def parse_csv_and_compute_monthly_averages(
    csv_data_by_year: dict[int, str],
) -> dict[int, dict[str, float]]:
    """
    Parse CSV data for multiple years and compute monthly averages.

    Args:
        csv_data_by_year: Dictionary mapping year to CSV data string

    Returns:
        Dictionary mapping month (1-12) to averages of precipitation, average daily temperature, number of sunny days
        Format: {1: {'precipitation': value, 'average_temp': value, 'sunny_days': value}, ...}
    """
    # Store all values by month
    monthly_data = {
        month: {"precipitation": [], "average_temp": [], "sunny_days": []}
        for month in range(1, 13)
    }

    for year, csv_data in csv_data_by_year.items():
        if not csv_data:
            continue

        # Parse CSV (semicolon-separated)
        reader = csv.DictReader(io.StringIO(csv_data), delimiter=";")

        for row in reader:
            try:
                # Extract date and parse month
                date_str = row.get("DATE", "")
                if len(date_str) != 6:  # Should be YYYYMM
                    continue

                month = int(date_str[4:6])  # Extract MM

                # Extract values (handle empty strings and commas as decimal separators)
                rr_str = row.get("RR", "").replace(",", ".")
                tmm_str = row.get("TMM", "").replace(",", ".")
                nbsigma80_str = row.get("NBSIGMA80", "").replace(",", ".")

                if rr_str:
                    monthly_data[month]["precipitation"].append(float(rr_str))
                if tmm_str:
                    monthly_data[month]["average_temp"].append(float(tmm_str))
                if nbsigma80_str:
                    monthly_data[month]["sunny_days"].append(float(nbsigma80_str))

            except (ValueError, KeyError) as e:
                # Skip rows with parsing errors
                continue

    # Compute averages
    monthly_averages = {}
    for month in range(1, 13):
        monthly_averages[month] = {}
        for field in ["precipitation", "average_temp", "sunny_days"]:
            values = monthly_data[month][field]
            if values:
                monthly_averages[month][field] = sum(values) / len(values)
            else:
                monthly_averages[month][field] = None

    return monthly_averages


def compute_all_monthly_averages(
    node_to_csv_by_year: dict[int, dict[int, str]],
) -> dict[int, dict[int, dict[str, float]]]:
    """
    Compute monthly averages for all nodes.

    Args:
        node_to_csv_by_year: Dictionary mapping node_id to year to CSV data

    Returns:
        Dictionary mapping node_id to month to averages
    """
    node_to_monthly_averages = {}

    for node_id, csv_by_year in node_to_csv_by_year.items():
        monthly_averages = parse_csv_and_compute_monthly_averages(csv_by_year)
        node_to_monthly_averages[node_id] = monthly_averages

    return node_to_monthly_averages


def create_weather_data_table(cursor: sqlite3.Cursor) -> None:
    """
    Create the t_weather_data table and associated indexes.

    Args:
        cursor: SQLite database cursor
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS t_weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id INTEGER NOT NULL,
            weather_station_id INTEGER NOT NULL,
            month INTEGER NOT NULL,
            precipitation REAL,
            average_temp REAL,
            sunny_days REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (node_id) REFERENCES t_nodes(id),
            FOREIGN KEY (weather_station_id) REFERENCES t_weather_station(id),
            UNIQUE(node_id, month)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_weather_data_node ON t_weather_data(node_id)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_weather_data_station ON t_weather_data(weather_station_id)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_weather_data_month ON t_weather_data(month)
    """)


def insert_weather_data(
    db_path: Path,
    node_to_monthly_averages: dict[int, dict[int, dict[str, float]]],
    node_to_station: dict[int, tuple[int, str, float]],
) -> None:
    """
    Insert monthly weather averages into the database.

    Args:
        db_path: Path to SQLite database
        node_to_monthly_averages: Dictionary mapping node_id to month to averages
        node_to_station: Dictionary mapping node_id to (weather_station_id, station_id, distance)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table
    print("Creating table t_weather_data...")
    create_weather_data_table(cursor)

    # Insert data
    print("Inserting weather data...")
    inserted_count = 0

    for node_id, monthly_averages in node_to_monthly_averages.items():
        # Get weather station id for this node
        if node_id not in node_to_station:
            print(
                f"Warning: No weather station found for node {node_id}", file=sys.stderr
            )
            continue

        weather_station_id, _, _ = node_to_station[node_id]

        for month, averages in monthly_averages.items():
            cursor.execute(
                """
                INSERT OR REPLACE INTO t_weather_data
                (node_id, weather_station_id, month, precipitation, average_temp, sunny_days)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    node_id,
                    weather_station_id,
                    month,
                    averages.get("precipitation"),
                    averages.get("average_temp"),
                    averages.get("sunny_days"),
                ),
            )
            inserted_count += 1

        # Commit every 100 nodes
        if inserted_count % 1200 == 0:  # 100 nodes * 12 months
            conn.commit()

    # Final commit
    conn.commit()
    conn.close()

    print(f"✓ Inserted {inserted_count} weather data records")


def main() -> None:
    import argparse

    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Fetch weather data for each node from closest weather station (years 2020-2025)"
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

    api_token = os.getenv("METEO_FRANCE_API_KEY")
    if not api_token:
        print(
            "Error: API token required. Set METEO_FRANCE_API_KEY in .env file",
            file=sys.stderr,
        )
        sys.exit(1)

    # Fetch weather data for years 2020-2025
    node_to_csv_by_year, node_to_station = fetch_weather_data_for_nodes(
        args.db, api_token
    )

    # Compute monthly averages
    print("\nComputing monthly averages across years...")
    node_to_monthly_averages = compute_all_monthly_averages(node_to_csv_by_year)
    print(f"Computed monthly averages for {len(node_to_monthly_averages)} nodes")

    # Insert into database
    print("\nInserting weather data into database...")
    insert_weather_data(args.db, node_to_monthly_averages, node_to_station)

    return node_to_monthly_averages


if __name__ == "__main__":
    main()

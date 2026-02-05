# /// script
# requires-python = ">=3.14"
# dependencies = []
# ///

import math
import sqlite3
import sys
from pathlib import Path


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
        Dictionary mapping node_id to (weather_station_id, station_id, distance_km)
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
        closest_station_name = None

        for ws_id, station_id, station_name, station_lat, station_lon in dept_stations:
            distance = haversine_distance(node_lat, node_lon, station_lat, station_lon)
            if distance < min_distance:
                min_distance = distance
                closest_station_id = ws_id
                closest_station_name = station_id

        # Store mapping
        if closest_station_id:
            node_to_station[node_id] = (
                closest_station_id,
                closest_station_name,
                min_distance,
            )

    conn.close()

    if no_station_count > 0:
        print(
            f"Warning: {no_station_count} nodes could not be matched to a weather station",
            file=sys.stderr,
        )

    return node_to_station


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Find closest weather station for each node"
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

    node_to_station = find_closest_weather_stations(args.db)
    return node_to_station


if __name__ == "__main__":
    main()

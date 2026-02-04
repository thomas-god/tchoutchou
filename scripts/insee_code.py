# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "httpx>=0.28.1",
# ]
# ///

import httpx
import json
import sys
import time
from pathlib import Path


def get_city_from_coordinates(lat: float, lon: float) -> dict | None:
    """
    Get city information from latitude/longitude coordinates using the French API.

    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate

    Returns:
        Dictionary containing city information or None if not found
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
        return None
    except httpx.HTTPError as e:
        print(f"HTTP error occurred: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        return None


def enrich_cities_from_file(input_file: Path, output_file: Path) -> None:
    """
    Load a JSON file with cities, enrich each with API data, and save to a new file.

    Args:
        input_file: Path to input JSON file
        output_file: Path to output JSON file
    """
    # Load the input file
    print(f"Loading {input_file}...")
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    total_entries = len(data)
    enriched_count = 0
    skipped_count = 0
    error_count = 0

    # Rate limiting: 50 calls/s = 0.02s per call
    rate_limit_delay = 0.02

    print(f"Processing {total_entries} entries...")
    print(f"Rate limit: 50 calls/s (waiting {rate_limit_delay}s between calls)")

    for i, entry in enumerate(data):
        # Skip empty entries
        if not entry or len(entry) < 2:
            skipped_count += 1
            continue

        stop_id, stop_data = entry

        # Skip if no coordinates
        if not stop_data or "lat" not in stop_data or "lon" not in stop_data:
            skipped_count += 1
            continue

        try:
            lat = float(stop_data["lat"])
            lon = float(stop_data["lon"])

            # Get city information from API
            city_info = get_city_from_coordinates(lat, lon)

            if city_info:
                # Enrich the stop data with city information
                stop_data["insee_code"] = city_info.get("code")
                stop_data["city_name"] = city_info.get("nom")
                stop_data["department_code"] = city_info.get("codeDepartement")
                stop_data["region_code"] = city_info.get("codeRegion")
                stop_data["population"] = city_info.get("population")
                stop_data["postal_codes"] = city_info.get("codesPostaux", [])
                enriched_count += 1
            else:
                error_count += 1

            # Rate limiting
            time.sleep(rate_limit_delay)

            # Progress update every 50 entries
            if (i + 1) % 50 == 0:
                print(
                    f"Progress: {i + 1}/{total_entries} | Enriched: {enriched_count} | Errors: {error_count} | Skipped: {skipped_count}"
                )

        except (ValueError, KeyError) as e:
            print(f"Error processing entry {i}: {e}", file=sys.stderr)
            error_count += 1

    # Write enriched data to output file
    print(f"\nWriting enriched data to {output_file}...")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\nDone!")
    print(f"  Total entries: {total_entries}")
    print(f"  Enriched: {enriched_count}")
    print(f"  Errors: {error_count}")
    print(f"  Skipped: {skipped_count}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Enrich city data with INSEE codes from geo.api.gouv.fr"
    )
    parser.add_argument("input_file", type=Path, help="Input JSON file with city data")
    parser.add_argument(
        "output_file", type=Path, help="Output JSON file for enriched data"
    )

    args = parser.parse_args()

    if not args.input_file.exists():
        print(f"Error: Input file {args.input_file} does not exist", file=sys.stderr)
        sys.exit(1)

    enrich_cities_from_file(args.input_file, args.output_file)


if __name__ == "__main__":
    main()

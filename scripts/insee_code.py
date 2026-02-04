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
    failures = []  # Track failed entries with reasons

    # Rate limiting: 50 calls/s = 0.02s per call
    rate_limit_delay = 0.02

    print(f"Processing {total_entries} entries...")
    print(f"Rate limit: 50 calls/s (waiting {rate_limit_delay}s between calls)")

    for i, entry in enumerate(data):
        # Skip empty entries
        if not entry or len(entry) < 2:
            failures.append({"index": i, "stop_id": None, "reason": "Empty entry"})
            skipped_count += 1
            continue

        stop_id, stop_data = entry

        # Skip if no coordinates
        if not stop_data or "lat" not in stop_data or "lon" not in stop_data:
            failures.append(
                {
                    "index": i,
                    "stop_id": stop_id,
                    "stop_name": stop_data.get("name") if stop_data else None,
                    "reason": "Missing coordinates",
                }
            )
            skipped_count += 1
            continue

        try:
            lat = float(stop_data["lat"])
            lon = float(stop_data["lon"])

            # Get city information from API
            result = get_city_from_coordinates(lat, lon)

            if isinstance(result, dict):
                # Enrich the stop data with city information
                stop_data["insee_code"] = result.get("code")
                stop_data["city_name"] = result.get("nom")
                stop_data["department_code"] = result.get("codeDepartement")
                stop_data["region_code"] = result.get("codeRegion")
                stop_data["population"] = result.get("population")
                stop_data["postal_codes"] = result.get("codesPostaux", [])
                enriched_count += 1
            else:
                # result is (None, error_message)
                _, error_message = result
                failures.append(
                    {
                        "index": i,
                        "stop_id": stop_id,
                        "stop_name": stop_data.get("name"),
                        "lat": lat,
                        "lon": lon,
                        "reason": error_message,
                    }
                )
                error_count += 1

            # Rate limiting
            time.sleep(rate_limit_delay)

            # Progress update every 50 entries
            if (i + 1) % 50 == 0:
                print(
                    f"Progress: {i + 1}/{total_entries} | Enriched: {enriched_count} | Errors: {error_count} | Skipped: {skipped_count}"
                )

        except (ValueError, KeyError) as e:
            failures.append(
                {
                    "index": i,
                    "stop_id": stop_id,
                    "stop_name": stop_data.get("name") if stop_data else None,
                    "reason": f"Exception: {type(e).__name__}: {str(e)}",
                }
            )
            print(f"Error processing entry {i}: {e}", file=sys.stderr)
            error_count += 1

    # Write enriched data to output file
    print(f"\nWriting enriched data to {output_file}...")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # Write failures report if there are any
    if failures:
        failures_file = output_file.parent / f"{output_file.stem}_failures.json"
        print(f"Writing failures report to {failures_file}...")
        with open(failures_file, "w", encoding="utf-8") as f:
            json.dump(failures, f, ensure_ascii=False, indent=2)

    print(f"\nDone!")
    print(f"  Total entries: {total_entries}")
    print(f"  Enriched: {enriched_count}")
    print(f"  Errors: {error_count}")
    print(f"  Skipped: {skipped_count}")

    if failures:
        print(f"\n⚠️  Failed entries saved to: {failures_file}")
        print(f"\nFailure breakdown:")
        failure_reasons = {}
        for failure in failures:
            reason = failure["reason"]
            failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
        for reason, count in sorted(
            failure_reasons.items(), key=lambda x: x[1], reverse=True
        ):
            print(f"  - {reason}: {count}")


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

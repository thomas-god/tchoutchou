# /// script
# requires-python = ">=3.14"
# dependencies = []
# ///

import json
import sqlite3
from pathlib import Path


def main() -> None:
    # Paths
    script_dir = Path(__file__).parent
    json_file = script_dir / "nodes.json"
    db_file = script_dir / "nodes.db"

    # Read JSON data
    print(f"Reading {json_file}...")
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Connect to SQLite database
    print(f"Connecting to {db_file}...")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create table
    print("Creating table t_nodes...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS t_nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sncf_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL
        );
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_sncf_id ON t_nodes(sncf_id);
    """)

    # Insert data
    print("Inserting nodes...")
    inserted = 0
    skipped = 0

    for entry in data:
        # Skip empty entries
        if len(entry) < 2 or not entry[1]:
            skipped += 1
            continue

        node = entry[1]
        cursor.execute(
            "INSERT OR REPLACE INTO t_nodes (sncf_id, name, lat, lon) VALUES (?, ?, ?, ?)",
            (node["id"], node["name"], float(node["lat"]), float(node["lon"])),
        )
        inserted += 1

    # Commit and close
    conn.commit()
    conn.close()

    print(f"✓ Inserted {inserted} nodes (skipped {skipped} empty entries)")
    print(f"✓ Database saved to {db_file}")


if __name__ == "__main__":
    main()

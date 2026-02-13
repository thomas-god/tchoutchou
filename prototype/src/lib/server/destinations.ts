import { DatabaseSync, StatementSync } from 'node:sqlite';
import { getEnv } from '$lib/env';
import type { Node } from '$lib/api/schedule';

let _db: DatabaseSync | null = null;
let _data: StatementSync | null;

const getDb = () => {
	if (!_db) {
		const path = getEnv('VITE_DESTINATIONS_DATABASE_PATH');
		_db = new DatabaseSync(path);
		initZonesTables(_db);
	}
	if (!_data) {
		_data = _db.prepare(`
    WITH city AS (SELECT t_nodes.sncf_id, population, COALESCE(postal_codes, json('[]')) AS postal_codes
    FROM t_nodes
    LEFT JOIN t_insee ON t_nodes.id = t_insee.node_id
    WHERE t_nodes.sncf_id = ?),
		city_zones AS (
			SELECT
				sncf_id,
				json_group_array(json_object('category', t_nodes_zones.category, 'name', t_nodes_zones.name)) AS zones
			FROM t_nodes_zones
			GROUP BY sncf_id
		)
    SELECT MAX(population) AS population, SUM(COALESCE(museum_count, 0)) AS museum, city_zones.zones
    FROM city
    LEFT JOIN json_each(city.postal_codes)
    LEFT JOIN t_museum ON t_museum.postal_code = value
		LEFT JOIN city_zones ON city_zones.sncf_id = city.sncf_id
    GROUP BY city.sncf_id;`);
	}

	return { db: _db, data: _data };
};

const initZonesTables = (db: DatabaseSync) => {
	db.exec(`
		CREATE VIRTUAL TABLE IF NOT EXISTS t_zones
		USING geopoly(id, category, name);
		`);

	// No explicit FK as t_zones is a virtual table and does not allow FK on it
	// (as virtual table data are beyond the control of SQLite, so FK cannot be enforced).
	// Referential integrity is thus enforced client-side in upsert and delete functions.
	db.exec(`
		CREATE TABLE IF NOT EXISTS t_nodes_zones (
			sncf_id TEXT,
			zone_id TEXT,
			category TEXT,
			name TEXT,
			PRIMARY KEY (sncf_id, zone_id)
		);
	`);
};

export interface EnrichedNode extends Node {
	population: number | null;
	numberOfMuseums: number | null;
	zones: null | {categoy: string, name: string}[]
}

export const enrichNode = (node: Node): EnrichedNode | null => {
	const { data } = getDb();

	const res = data.get(node.id);
	if (res === undefined) {
		return null;
	}
	return {
		...node,
		population: res['population'] as number,
		numberOfMuseums: res['museum'] as number,
		zones: res["zones"] as any
	};
};

// Core zone data type (serializable)
export const zoneCategories = ['sea', 'mountain'] as const;
export type ZoneCategory = (typeof zoneCategories)[number];
export interface Zone {
	id: string;
	name: string;
	category: ZoneCategory;
	coordinates: { lat: number; lng: number }[];
}

export const upsertZone = (zone: Zone) => {
	const { db } = getDb();

	// Convert coordinates to geopoly JSON format [[lng, lat], ...] and close the polygon
	const coords = zone.coordinates.map((c) => [c.lng, c.lat]);
	coords.push(coords[0]); // Close the polygon
	const shape = JSON.stringify(coords);

	db.exec('BEGIN TRANSACTION');
	try {
		// Delete existing zone and its associations
		db.prepare('DELETE FROM t_zones WHERE id = ?').run(zone.id);
		db.prepare('DELETE FROM t_nodes_zones WHERE zone_id = ?').run(zone.id);

		// Insert new zone
		db.prepare('INSERT INTO t_zones (_shape, id, category, name) VALUES (?, ?, ?, ?)').run(
			shape,
			zone.id,
			zone.category,
			zone.name
		);

		// Update t_nodes_zones for nodes within this zone
		const updateStmt = db.prepare(`
			INSERT OR IGNORE INTO t_nodes_zones (sncf_id, zone_id, category, name)
			SELECT
				t_nodes.sncf_id,
				?,
				?,
				?
			FROM t_nodes
			WHERE geopoly_contains_point(
				(SELECT _shape FROM t_zones WHERE id = ?),
				t_nodes.lon,
				t_nodes.lat
			) > 0
		`);
		updateStmt.run(zone.id, zone.category, zone.name, zone.id);

		db.exec('COMMIT');
	} catch (error) {
		db.exec('ROLLBACK');
		throw error;
	}
};

export const getZones = (): Zone[] => {
	const { db } = getDb();

	const stmt = db.prepare('SELECT id, category, name, geopoly_json(_shape) as shape FROM t_zones');
	const rows = stmt.all();

	return rows.map((row) => {
		const coords = JSON.parse(row.shape as string) as [number, number][];
		// Remove the last coordinate (duplicate of first, used to close the polygon for geopoly extension)
		coords.pop();

		return {
			id: row.id as string,
			category: row.category as ZoneCategory,
			name: row.name as string,
			coordinates: coords.map(([lng, lat]) => ({ lat, lng }))
		};
	});
};

export const deleteZone = (id: string | undefined) => {
	if (!id) throw new Error('Zone ID is required for deletion');
	const { db } = getDb();

	db.exec('BEGIN TRANSACTION');
	try {
		// Delete from t_nodes_zones first
		db.prepare('DELETE FROM t_nodes_zones WHERE zone_id = ?').run(id);
		// Then delete the zone itself
		db.prepare('DELETE FROM t_zones WHERE id = ?').run(id);
		db.exec('COMMIT');
	} catch (error) {
		db.exec('ROLLBACK');
		throw error;
	}
};

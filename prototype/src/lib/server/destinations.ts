import { DatabaseSync, StatementSync } from 'node:sqlite';
import { getEnv } from '$lib/env';
import type { Node } from '$lib/api/schedule';

let _db: DatabaseSync | null = null;
let _data: StatementSync | null;

const getDb = () => {
	if (!_db) {
		const path = getEnv('VITE_DESTINATIONS_DATABASE_PATH');
		_db = new DatabaseSync(path);
	}
	if (!_data) {
		_data = _db.prepare(`
    WITH city AS (select sncf_id, population, COALESCE(postal_codes, json('[]')) AS postal_codes
    FROM t_nodes
    LEFT JOIN t_insee ON t_nodes.id = t_insee.node_id
    WHERE sncf_id = ?)
    SELECT MAX(population) AS population, SUM(COALESCE(museum_count, 0)) AS museum
    FROM city
    LEFT JOIN json_each(city.postal_codes)
    LEFT JOIN t_museum ON t_museum.postal_code = value
    GROUP BY sncf_id;`);
	}

	_db.exec(`
		CREATE VIRTUAL TABLE IF NOT EXISTS t_zones
		USING geopoly(id, category, name);
		`);

	return { db: _db, data: _data };
};

export interface EnrichedNode extends Node {
	population: number | null;
	numberOfMuseums: number | null;
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
		numberOfMuseums: res['museum'] as number
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
		// Delete existing if present
		db.prepare('DELETE FROM t_zones WHERE id = ?').run(zone.id);

		// Insert new
		db.prepare('INSERT INTO t_zones (_shape, id, category, name) VALUES (?, ?, ?, ?)').run(
			shape,
			zone.id,
			zone.category,
			zone.name
		);

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
		// Remove the last coordinate (duplicate of first, used to close the polygon)
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
	db.prepare('DELETE FROM t_zones WHERE id = ?').run(id);
};

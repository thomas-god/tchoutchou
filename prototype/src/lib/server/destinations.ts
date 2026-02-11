import { DatabaseSync } from 'node:sqlite';
import { getEnv } from '$lib/env';
import type { Node } from '$lib/schedule';

let _db: DatabaseSync | null = null;

const getDb = () => {
	if (!_db) {
		const path = getEnv('VITE_DESTINATIONS_DATABASE_PATH');
		_db = new DatabaseSync(path);
	}

	return _db;
};

export interface EnrichedNode extends Node {
	population: number;
	numberOfMuseums: number;
}

export const enrichNodes = (nodes: Node[]): EnrichedNode[] => {
	const db = getDb();

	const data = db.prepare(`
    WITH city AS (select sncf_id, population, COALESCE(postal_codes, json('[]')) AS postal_codes
    FROM t_nodes
    LEFT JOIN t_insee ON t_nodes.id = t_insee.node_id
    WHERE sncf_id = ?)
    SELECT MAX(population) AS population, SUM(COALESCE(museum_count, 0)) AS museum
    FROM city
    LEFT JOIN json_each(city.postal_codes)
    LEFT JOIN t_museum ON t_museum.postal_code = value
    GROUP BY sncf_id;`);

	const enrichedNodes: EnrichedNode[] = [];
	for (const node of nodes) {
		const res = data.get(node.id);
		if (res === undefined) {
			continue;
		}
		enrichedNodes.push({
			...node,
			population: res['population'] as number,
			numberOfMuseums: res['museum'] as number
		});
	}

	return enrichedNodes;
};

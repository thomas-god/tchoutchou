import { query } from '$app/server';
import { type Line } from './api/lines';
import { getDb } from './server/db';

export const fetchLinesQuery = query(async (): Promise<Line[]> => {
	const db = getDb();
	const select = db.prepare('SELECT id, name, direction FROM t_lines;');
	return select.all().map((row) => ({
		id: row.id as string,
		name: row.name as string,
		direction: row.direction as string
	}));
});

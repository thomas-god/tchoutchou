import { fetchLines } from '$lib/api/lines';
import { getDb, initDB } from '$lib/server/db';
import type { ServerInit } from '@sveltejs/kit';

export const init: ServerInit = async () => {
	const db = getDb();
	initDB(db);

	// Load lines into DB
	const { n_lines } = db.prepare('select count(id) as n_lines from t_lines;').get() as {
		n_lines: number;
	};
	if (n_lines === 0) {
		console.log('Loading lines into database');
		const lines = await fetchLines();
		const insert = db.prepare('INSERT INTO t_lines (id, name, direction) VALUES (?, ?, ?)');
		for (const line of lines) {
			insert.run(line.id, line.name, line.direction);
		}
		console.log(`${lines.length} lines loaded in database`);
	} else {
		console.log(`${n_lines} lines already loaded in database, skipping initialisation`);
	}
};

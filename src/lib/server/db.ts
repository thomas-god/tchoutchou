import { DatabaseSync } from 'node:sqlite';
import { getEnv } from '$lib/env';

let _db: DatabaseSync | null = null;

export const getDb = () => {
	if (!_db) {
		const path = getEnv('VITE_DATABASE_PATH');
		_db = new DatabaseSync(path);

		_db.exec(`
      PRAGMA journal_mode = WAL;
    `);
	}

	return _db;
};

export const initDB = (db: DatabaseSync) => {
	db.exec(
		'CREATE TABLE IF NOT EXISTS t_lines (id TEXT UNIQUE PRIMARY KEY, name TEXT, direction TEXT);'
	);
};

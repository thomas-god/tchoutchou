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
		`CREATE TABLE IF NOT EXISTS t_lines (
			id TEXT UNIQUE PRIMARY KEY,
			name TEXT,
			direction TEXT
		);`
	);

	db.exec(
		`CREATE TABLE IF NOT EXISTS t_schedules (
			id TEXT UNIQUE PRIMARY KEY,
			route TEXT,
			direction TEXT,
			headsign TEXT,
			date TEXT
		);
		CREATE INDEX IF NOT EXISTS t_schedules_index_date_id ON t_schedules (date, id);`
	);

	db.exec(
		`CREATE TABLE IF NOT EXISTS t_stops (
			id TEXT,
			schedule_id TEXT,
			name TEXT,
			datetime TEXT,
			FOREIGN KEY(schedule_id) REFERENCES t_schedules(id)
		);
		CREATE INDEX IF NOT EXISTS t_stops_index_id ON t_stops (id);`
	);
};

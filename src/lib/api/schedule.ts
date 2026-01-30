import { getEnv } from '$lib/env';
import { getDb } from '$lib/server/db';
import dayjs from 'dayjs';
import z from 'zod';

export type LineSchedule = Schedule[];

export interface Schedule {
	id: string;
	route: string;
	direction: string;
	headsign: string;
	date: string;
	stops: ScheduleStop[];
}

export interface ScheduleStop {
	id: string;
	name: string;
	lat: number;
	lon: number;
	date_time: string;
}

export const persistLineSchedules = async (date: dayjs.Dayjs) => {
	const db = getDb();

	const n_schedules = (
		db
			.prepare('SELECT COUNT(*) as count FROM t_schedules WHERE date = ?')
			.get(date.format('YYYYMMDD')) as { count: number }
	).count;

	if (n_schedules === 0) {
		console.log(`Loading schedules for ${date.format('YYYY-MM-DD')}`);
		const lines = db.prepare('SELECT id FROM t_lines ORDER BY NAME;').all() as {
			id: string;
		}[];

		for (const line of lines) {
			const schedules = await fetchLineSchedule({ line: line.id, from: date.toDate() });
			for (const schedule of schedules) {
				db.prepare(
					'INSERT INTO t_schedules (id, route, direction, headsign, date) VALUES (?, ?, ?, ?, ?)'
				).run(schedule.id, schedule.route, schedule.direction, schedule.headsign, schedule.date);
				for (const stop of schedule.stops) {
					db.prepare(
						`INSERT INTO t_stops
							(id, schedule_id, name, lat, lon, datetime)
							VALUES (?, ?, ?, ?, ?, ?);`
					).run(stop.id, schedule.id, stop.name, stop.lat, stop.lon, stop.date_time);
				}
				console.log(`Inserted schedule ${schedule.id}`);
			}
		}
	}
};

export const schema = z.object({
	line: z.string(),
	from: z.date().optional()
});
type Schema = z.infer<typeof schema>;

export const fetchLineSchedule = async ({ line, from }: Schema): Promise<LineSchedule> => {
	const fromDate = from === undefined ? dayjs() : dayjs(from);
	const res = await fetch(
		`https://api.navitia.io/v1/coverage/sncf/routes/${line}/route_schedules?from_datetime=${fromDate.format('YYYYMMDDTHHmmss')}`,

		{
			headers: {
				Authorization: getEnv('VITE_API_KEY')
			}
		}
	);
	const data = await res.json();

	if (data.route_schedules.length === 0) {
		return [];
	}

	const line_schedule = data.route_schedules[0].table.headers.map((journey: any) => ({
		id: journey.links.find((link: any) => link.type === 'vehicle_journey').id,
		route: data.route_schedules[0].display_informations.name,
		direction: journey.display_informations.direction,
		headsign: journey.display_informations.headsign,
		date: fromDate.format('YYYYMMDD'),
		stops: []
	}));

	for (const stop of data.route_schedules[0].table.rows) {
		for (const [journey_index, date_time] of stop.date_times.entries()) {
			if (date_time.date_time !== '') {
				line_schedule[journey_index].stops.push({
					id: stop.stop_point.id,
					name: stop.stop_point.name,
					lat: stop.stop_point.coord.lat,
					lon: stop.stop_point.coord.lon,
					date_time: date_time.date_time
				});
			}
		}
	}

	return line_schedule;
};

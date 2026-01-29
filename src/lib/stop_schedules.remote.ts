import { query } from '$app/server';
import dayjs from 'dayjs';
import { persistLineSchedules, type LineSchedule } from './api/schedule';
import z from 'zod';
import { getDb } from './server/db';

const schema = z.object({
	stop: z.string(),
	from: z.string()
});

export const fetchStopSchedulesQuery = query(
	schema,
	async ({ stop, from }): Promise<LineSchedule> => {
		const fromDate = dayjs(from);
		await persistLineSchedules(fromDate);

		const db = getDb();
		const schedules = db
			.prepare(
				`SELECT
          t_schedules.id,
          t_schedules.route,
          t_schedules.headsign,
          t_stops.datetime,
          json_group_array(
            json_object(
              'id', s.id,
              'datetime', s.datetime,
              'name', s.name
            )
          ) FILTER (WHERE s.datetime >= t_stops.datetime) AS stops
        FROM t_stops
        INNER JOIN t_schedules ON t_stops.schedule_id = t_schedules.id
        LEFT JOIN t_stops s ON t_schedules.id = s.schedule_id
        WHERE t_stops.id = ? AND t_schedules.date = ?
        GROUP BY t_schedules.id, t_schedules.route, t_schedules.headsign;`
			)
			.all(stop, fromDate.format('YYYYMMDD'));

		return schedules
			.map((schedule) => ({
				id: schedule.id,
				route: schedule.route,
				direction: schedule.direction,
				headsign: schedule.headsign,
				date: from,
				stops: JSON.parse(schedule.stops)
					.map((stop) => ({
						id: stop.id,
						name: stop.name,
						date_time: stop.datetime
					}))
					.toSorted((a, b) => (a.date_time > b.date_time ? 1 : -1))
			}))
			.filter((schedule) => schedule.stops.length > 1)
			.toSorted((a, b) => (a.stops[0].date_time > b.stops[0].date_time ? 1 : -1));
	}
);

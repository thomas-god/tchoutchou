import { query } from '$app/server';
import { getEnv } from '$lib/env';
import dayjs from 'dayjs';
import z from 'zod';

export type LineSchedule = Schedule[];

export interface Schedule {
	id: string;
	route: string;
	direction: string;
	headsign: string;
	stops: ScheduleStop[];
}

export interface ScheduleStop {
	id: string;
	name: string;
	date_time: string;
}

const schema = z.object({
	line: z.string(),
	date: z.date().optional()
});

export const fetchLineSchedule = query(schema, async ({ line, date }): Promise<LineSchedule> => {
	const from = date === undefined ? dayjs() : dayjs(date);
	const res = await fetch(
		`https://api.navitia.io/v1/coverage/sncf/routes/${line}/route_schedules?from_datetime=${from.format('YYYYMMDDTHHmmss')}`,

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
		stops: []
	}));

	for (const stop of data.route_schedules[0].table.rows) {
		for (const [journey_index, date_time] of stop.date_times.entries()) {
			if (date_time.date_time !== '') {
				line_schedule[journey_index].stops.push({
					id: stop.stop_point.id,
					name: stop.stop_point.name,
					date_time: date_time.date_time
				});
			}
		}
	}

	return line_schedule;
});

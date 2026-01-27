import { query } from '$app/server';
import { getEnv } from '$lib/env';
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

export const fetchLineSchedule = query(z.string(), async (line): Promise<LineSchedule> => {
	const res = await fetch(
		`https://api.navitia.io/v1/coverage/sncf/routes/${line}/route_schedules`,

		{
			headers: {
				Authorization: getEnv('VITE_API_KEY')
			}
		}
	);
	const data = await res.json();

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

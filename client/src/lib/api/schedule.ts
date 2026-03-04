import { getEnv } from '$lib/env';
import { getDb } from '$lib/server/db';
import dayjs from 'dayjs';
import { existsSync } from 'node:fs';
import { readFile, writeFile } from 'node:fs/promises';
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

export interface Graph {
	nodes: Map<string, Node>;
	edgesByNode: Map<string, Edge[]>;
}

export const getEdgesAndNodes = async (date: dayjs.Dayjs): Promise<Graph> => {
	const location = getEnv('VITE_DATA_PATH');
	const edgesFile = `${location}/${date.format('YYYYMMDD')}_edges.json`;
	const nodesFile = `${location}/${date.format('YYYYMMDD')}_nodes.json`;
	if (!existsSync(edgesFile) || !existsSync(nodesFile)) {
		console.log(`No edges/nodes files found for ${date.format('YYYYMMDD')}, fetching data`);
		return await persistEdgesAndNodes(date);
	}

	const nodes: Map<string, Node> = new Map(
		JSON.parse(await readFile(nodesFile, { encoding: 'utf-8' }))
	);
	const edgesByNode: Map<string, Edge[]> = new Map(
		JSON.parse(await readFile(edgesFile, { encoding: 'utf-8' }))
	);

	return { nodes, edgesByNode };
};

export const persistEdgesAndNodes = async (date: dayjs.Dayjs): Promise<Graph> => {
	const db = getDb();

	const lines = db.prepare('SELECT id FROM t_lines ORDER BY NAME;').all() as {
		id: string;
	}[];

	let nodes: Map<string, Node> = new Map();
	let edges: Edge[] = [];
	for (const line of lines) {
		const schedules = await fetchLineSchedule({ line: line.id, from: date.toDate() });
		const { nodes: _nodes, edges: _edges } = splitScheduleIntoNodeAndEdges(schedules);
		edges = [...edges, ..._edges];
		for (const [id, node] of _nodes.entries()) {
			nodes.set(id, node);
		}
	}

	const edgesByNode = mergeEdgesByNode(edges);
	const location = getEnv('VITE_DATA_PATH');
	await writeFile(
		`${location}/${date.format('YYYYMMDD')}_edges.json`,
		JSON.stringify([...edgesByNode])
	);
	await writeFile(`${location}/${date.format('YYYYMMDD')}_nodes.json`, JSON.stringify([...nodes]));

	console.log(`${nodes.size} nodes and ${edges.length} edges persisted`);
	return { nodes, edgesByNode };
};

export interface Node {
	id: string;
	name: string;
	lat: number;
	lon: number;
}

export interface Edge {
	origin: string;
	destination: string;
	departure: string;
	departureDt: number;
	arrival: string;
	arrivalDt: number;
	intermediaryStops: string[];
	route: string;
}

export const splitScheduleIntoNodeAndEdges = (
	schedule: LineSchedule
): { nodes: Map<string, Node>; edges: Edge[] } => {
	const nodes = new Map();
	const edges = [];

	for (const trip of schedule) {
		const stops = trip.stops.toSorted((a, b) => (a.date_time > b.date_time ? 1 : -1));
		for (const [originIdx, origin] of stops.entries()) {
			nodes.set(origin.id, {
				id: origin.id,
				name: origin.name,
				lat: origin.lat,
				lon: origin.lon
			});
			// Exclude origin from possible destinations
			for (const [destinationIdx, destination] of stops.slice(originIdx + 1).entries()) {
				// End of slice is : originIdx + 1 as destinationIdx is in the referential of
				// stops.slice(originIdx + 1) not the original stops array; and hen destinationIdx + 1
				// to include the destination in the intermediary stops.
				const intermediaryStops = stops
					.slice(originIdx, originIdx + 1 + destinationIdx + 1)
					.map((stop) => stop.id);
				edges.push({
					origin: origin.id,
					destination: destination.id,
					departure: origin.date_time,
					departureDt: dayjs(origin.date_time).unix(),
					arrival: destination.date_time,
					arrivalDt: dayjs(destination.date_time).unix(),
					route: trip.id,
					intermediaryStops
				});
			}
		}
	}

	return { nodes, edges };
};

export const mergeEdgesByNode = (edges: Edge[]): Map<string, Edge[]> => {
	const edgesByNode = new Map();

	for (const edge of edges) {
		if (!edgesByNode.has(edge.origin)) {
			edgesByNode.set(edge.origin, []);
		}
		edgesByNode.get(edge.origin)!.push(edge);
	}

	return edgesByNode;
};

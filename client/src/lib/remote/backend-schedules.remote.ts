import { getEnv } from '$lib/env';
import { query } from '$app/server';
import z from 'zod';

// --- Shared types ---

const backendStationSchema = z.object({
	id: z.number(),
	name: z.string(),
	lat: z.number(),
	lon: z.number()
});

export type BackendStation = z.infer<typeof backendStationSchema>;

export interface BackendDestinationResult {
	station: BackendStation;
	duration: number;
	connections: number;
	visitedStations: BackendStation[];
}

export interface BackendDestinationsResult {
	origin: BackendStation | null;
	destinations: BackendDestinationResult[];
}

// --- Autocomplete ---

export const autocompleteBackendStation = query(
	z.string(),
	async (q: string): Promise<{ id: number; name: string }[]> => {
		const backendUrl = getEnv('BACKEND_URL');
		const res = await fetch(
			`${backendUrl}/api/stations/autocomplete?substring=${encodeURIComponent(q)}`
		);
		const data = await res.json();
		return (data.stations ?? []) as { id: number; name: string }[];
	}
);

// --- Fetch destinations ---

const fetchParamsSchema = z.object({
	from: z.number(),
	date: z.string()
});

export const fetchBackendDestinations = query(
	fetchParamsSchema,
	async ({ from, date }): Promise<BackendDestinationsResult> => {
		const backendUrl = getEnv('BACKEND_URL');

		const yyyymmdd = date.replaceAll('-', '');

		const [stationsRes, destinationsRes] = await Promise.all([
			fetch(`${backendUrl}/api/stations`),
			fetch(`${backendUrl}/api/destinations?from=${from}&date=${yyyymmdd}`)
		]);

		const { stations } = (await stationsRes.json()) as {
			stations: BackendStation[];
		};
		const { destinations } = (await destinationsRes.json()) as {
			destinations: { station_id: number; duration: number; connections: number; visited_station_ids: number[] }[];
		};

		const stationMap = new Map(stations.map((s) => [s.id, s]));
		const origin = stationMap.get(from) ?? null;

		const results: BackendDestinationResult[] = destinations
			.filter((d) => stationMap.has(d.station_id))
			.map((d) => ({
				station: stationMap.get(d.station_id)!,
				duration: d.duration,
				connections: d.connections,
				visitedStations: (d.visited_station_ids ?? [])
					.filter((id) => stationMap.has(id))
					.map((id) => stationMap.get(id)!)
			}));

		return { origin, destinations: results };
	}
);

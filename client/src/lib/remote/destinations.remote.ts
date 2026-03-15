import { getEnv } from '$lib/env';
import { query } from '$app/server';
import z from 'zod';
import type { Destination, Trip } from '$lib/server/types';

// --- Shared types ---

export type ZoneCategory = 'sea' | 'mountain';

export interface Zone {
	category: ZoneCategory;
	name: string;
}

const citySchema = z.object({
	id: z.number(),
	name: z.string(),
	country: z.string(),
	lat: z.number(),
	lon: z.number()
});

export type City = z.infer<typeof citySchema> & {
	population?: number | null;
	numberOfMuseums?: number | null;
	zones?: Zone[];
};

export interface DestinationResult {
	station: City;
	duration: number;
	connections: number;
	visitedStations: City[];
}

export interface DestinationsResult {
	origin: City | null;
	destinations: DestinationResult[];
}

// --- Autocomplete ---

const autocompleteResponseSchema = z.object({
	id: z.number(),
	name: z.string()
});

export type AutocompleteStation = z.infer<typeof autocompleteResponseSchema>;

export const autocompleteStation = query(
	z.string(),
	async (q: string): Promise<AutocompleteStation[]> => {
		const url = getEnv('BACKEND_URL');
		const res = await fetch(`${url}/api/stations/autocomplete?substring=${encodeURIComponent(q)}`);
		const data = await res.json();
		return (data.stations ?? []) as AutocompleteStation[];
	}
);

// --- Fetch destinations ---

const fetchParamsSchema = z.object({
	from: z.number()
});

export const fetchDestinations = query(
	fetchParamsSchema,
	async ({ from }): Promise<DestinationsResult> => {
		const url = getEnv('BACKEND_URL');

		const destinationsRes = await fetch(`${url}/api/destinations?from=${from}`);

		const { destinations, cities } = (await destinationsRes.json()) as {
			destinations: {
				station_id: number;
				duration: number;
				connections: number;
				visited_station_ids: number[];
			}[];
			cities: City[];
		};

		const stationMap = new Map(cities.map((s) => [s.id, s]));
		const origin = stationMap.get(from) ?? null;

		const results: DestinationResult[] = destinations
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

export interface DestinationsQueryParams {
	origin: number;
	from: string;
	filters: {
		maxConnections: number;
		minDuration: number;
		maxDuration: number;
	};
}

function transformToDestination(result: DestinationResult): Destination {
	const city: City = {
		...result.station,
		population: null,
		numberOfMuseums: null,
		zones: []
	};

	const trip: Trip = {
		origin: result.visitedStations[0] || result.station,
		destination: result.station,
		duration: result.duration,
		connections: result.connections,
		visitedStations: result.visitedStations,
		legs: [],
		intermediaryStopNames: result.visitedStations.slice(1, -1).map((s) => s.name)
	};

	return { city, trip };
}

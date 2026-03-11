import type { City } from '$lib/remote/destinations.remote';

export interface Destination {
	city: City;
	trip: Trip;
}

export interface TripLeg {
	origin: City;
	destination: City;
	departure: number;
	arrival: number;
}

export interface Trip {
	origin: City;
	destination: City;
	duration: number;
	connections: number;
	visitedStations: City[];
	legs: TripLeg[];
	intermediaryStopNames: string[];
}

// Re-export for convenience
export type { City, Zone, ZoneCategory } from '$lib/remote/destinations.remote';

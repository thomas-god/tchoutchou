import { getEdgesAndNodes, type Graph } from '$lib/api/schedule';
import type { Edge } from '$lib/schedule';
import dayjs from 'dayjs';

const graphs: Map<string, Graph> = new Map();

export const getGraph = async (date: dayjs.Dayjs): Promise<Graph> => {
	let _date = date.format('YYYYMMDD');
	if (!graphs.has(_date)) {
		console.log(`Graph for date ${_date} not loaded in memory, fetching it...`);
		const graph = await getEdgesAndNodes(date);
		graphs.set(_date, graph);
	}
	return graphs.get(_date)!;
};

export interface Trip {
	destination: string;
	legs: TripLeg[];
}

export interface TripLeg {
	routeId: string;
	origin: string;
	destination: string;
	departure: string;
	arrival: string;
}

interface InternalTrip {
	visitedStops: string[];
	currentStop: string;
	current: dayjs.Dayjs;
	legs: TripLeg[];
}

const maxNumberOfConnections = 2;

const edgeToTripLeg = (edge: Edge): TripLeg => {
	return {
		routeId: edge.route,
		origin: edge.origin,
		destination: edge.destination,
		departure: edge.arrival,
		arrival: edge.arrival
	};
};

const internalTripToTrip = (internalTrip: InternalTrip): Trip => {
	return {
		destination: internalTrip.currentStop,
		legs: internalTrip.legs
	};
};

export const findTrips = async (origin: string, from: dayjs.Dayjs): Promise<Map<string, Trip>> => {
	const graph = await getGraph(from);

	const initialTrips: InternalTrip[] = [
		{
			currentStop: origin,
			current: from,
			visitedStops: [],
			legs: []
		}
	];

	const trips = _findTrips(initialTrips, graph).map(internalTripToTrip);

	return deduplicateTripsByDestination(trips);
};

const _findTrips = (trips: InternalTrip[], graph: Graph): InternalTrip[] => {
	const newTrips: InternalTrip[] = [];

	for (const trip of trips) {
		if (trip.legs.length < maxNumberOfConnections) {
			const possibleTrips = graph.edgesByNode.get(trip.currentStop) || [];
			for (const candidate of possibleTrips) {
				const canCatchCandidate = dayjs(candidate.departure) > trip.current;
				const notVisitedDestinationYet = !trip.visitedStops.includes(candidate.destination);
				if (canCatchCandidate && notVisitedDestinationYet) {
					newTrips.push({
						currentStop: candidate.destination,
						current: dayjs(candidate.arrival),
						visitedStops: [...trip.visitedStops, ...candidate.intermediaryStops],
						legs: [...trip.legs, edgeToTripLeg(candidate)]
					});
				}
			}
		}
	}

	if (newTrips.length > 0) {
		return [...trips, ..._findTrips(newTrips, graph)];
	} else {
		return trips;
	}
};

const tripDuration = (trip: Trip): number =>
	dayjs(trip.legs.at(-1)!.arrival).diff(dayjs(trip.legs.at(0)!.departure), 'second');

export const deduplicateTripsByDestination = (trips: Trip[]): Map<string, Trip> => {
	const bestTrips = new Map();

	for (const trip of trips) {
		if (trip.legs.length === 0) {
			continue;
		}

		const existingTrip = bestTrips.get(trip.destination);
		if (existingTrip === undefined) {
			bestTrips.set(trip.destination, trip);
		} else {
			if (tripDuration(trip) < tripDuration(existingTrip)) {
				bestTrips.set(trip.destination, trip);
			}
		}
	}

	return bestTrips;
};

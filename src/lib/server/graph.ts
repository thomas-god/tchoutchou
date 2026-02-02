import { getEdgesAndNodes, type Graph } from '$lib/api/schedule';
import type { Edge, Node } from '$lib/schedule';
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
	origin: string;
	destination: string;
	duration: number;
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

const edgeToTripLeg = (edge: Edge): TripLeg => {
	return {
		routeId: edge.route,
		origin: edge.origin,
		destination: edge.destination,
		departure: edge.departure,
		arrival: edge.arrival
	};
};

const internalTripToTrip = (internalTrip: InternalTrip): Trip => {
	const duration =
		internalTrip.legs.length > 0
			? dayjs(internalTrip.legs.at(-1)!.arrival).diff(
					dayjs(internalTrip.legs.at(0)!.departure),
					'second'
				)
			: -1;
	return {
		origin: internalTrip.legs.at(0)!.origin,
		destination: internalTrip.currentStop,
		duration: duration,
		legs: internalTrip.legs
	};
};

export const findDestinations = async (
	origin: string,
	from: dayjs.Dayjs,
	maxConnections = 1
): Promise<{ node: Node; trip: Trip }[]> => {
	const graph = await getGraph(from);
	const maxLegs = maxConnections < 3 ? maxConnections + 1 : 3;

	const initialTrips: InternalTrip[] = [
		{
			currentStop: origin,
			current: from,
			visitedStops: [],
			legs: []
		}
	];

	const trips = findTrips(initialTrips, graph, maxLegs)
		.filter((trip) => trip.legs.length > 0)
		.map(internalTripToTrip);

	return deduplicateTripsByDestination(trips, graph.nodes);
};

const findTrips = (trips: InternalTrip[], graph: Graph, maxLegs: number): InternalTrip[] => {
	const newTrips: InternalTrip[] = [];

	for (const trip of trips) {
		if (trip.legs.length < maxLegs) {
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
		return [...trips, ...findTrips(newTrips, graph, maxLegs)];
	} else {
		return trips;
	}
};

const tripDuration = (trip: Trip): number =>
	dayjs(trip.legs.at(-1)!.arrival).diff(dayjs(trip.legs.at(0)!.departure), 'second');

export const deduplicateTripsByDestination = (
	trips: Trip[],
	nodes: Map<string, Node>
): { node: Node; trip: Trip }[] => {
	const bestTrips: Map<string, { node: Node; trip: Trip }> = new Map();

	for (const trip of trips) {
		if (trip.legs.length === 0) {
			continue;
		}

		const existingTrip = bestTrips.get(trip.destination);
		if (existingTrip === undefined) {
			bestTrips.set(trip.destination, { node: nodes.get(trip.destination)!, trip });
		} else {
			if (tripDuration(trip) < tripDuration(existingTrip.trip)) {
				bestTrips.set(trip.destination, { node: nodes.get(trip.destination)!, trip });
			}
		}
	}

	return [...bestTrips.values()].sort((a, b) => (a.trip.duration > b.trip.duration ? 1 : -1));
};

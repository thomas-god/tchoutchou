import { getEdgesAndNodes, type Graph, type Edge, type Node } from '$lib/api/schedule';
import dayjs from 'dayjs';
import { enrichNode, type EnrichedNode } from './destinations';

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

interface InternalTripLeg extends TripLeg {
	departureDt: number;
	arrivalDt: number;
}

interface InternalTrip {
	visitedStops: string[];
	currentStop: string;
	current: number;
	legs: InternalTripLeg[];
}

const edgeToTripLeg = (edge: Edge): InternalTripLeg => {
	return {
		routeId: edge.route,
		origin: edge.origin,
		destination: edge.destination,
		departure: edge.departure,
		departureDt: edge.departureDt,
		arrival: edge.arrival,
		arrivalDt: edge.arrivalDt
	};
};

const internalTripToTrip = (internalTrip: InternalTrip): Trip => {
	const duration =
		internalTrip.legs.length > 0
			? internalTrip.legs.at(-1)!.arrivalDt - internalTrip.legs.at(0)!.departureDt
			: -1;
	return {
		origin: internalTrip.legs.at(0)!.origin,
		destination: internalTrip.currentStop,
		duration: duration,
		legs: internalTrip.legs
	};
};

export interface DestinationsFilters {
	maxConnections: number;
	minDuration: number;
	maxDuration: number;
}

export const findDestinations = async (
	origin: string,
	from: dayjs.Dayjs,
	filters: DestinationsFilters
): Promise<{ node: EnrichedNode; trip: Trip }[]> => {
	const graph = await getGraph(from);
	const maxLegs = filters.maxConnections < 3 ? filters.maxConnections + 1 : 3;

	const initialTrips: InternalTrip[] = [
		{
			currentStop: origin,
			current: from.unix(),
			visitedStops: [],
			legs: []
		}
	];

	const trips = findTrips(initialTrips, graph, maxLegs, filters.maxDuration)
		.filter((trip) => trip.legs.length > 0)
		.map(internalTripToTrip);

	return deduplicateTripsByDestination(trips, graph.nodes)
		.filter(({ trip }) => trip.duration >= filters.minDuration)
		.map(({ node, trip }) => ({ node: enrichNode(node), trip }))
		.filter((dest): dest is { node: EnrichedNode; trip: Trip } => dest.node !== null);
};

const findTrips = (
	trips: InternalTrip[],
	graph: Graph,
	maxLegs: number,
	maxDuration: number
): InternalTrip[] => {
	const newTrips: InternalTrip[] = [];

	for (const trip of trips) {
		if (trip.legs.length < maxLegs) {
			const possibleTrips = graph.edgesByNode.get(trip.currentStop) || [];
			for (const candidate of possibleTrips) {
				const canCatchCandidate = candidate.departureDt > trip.current;
				const notVisitedDestinationYet = !trip.visitedStops.includes(candidate.destination);
				const tripTotalDurationNotExceeded = expectedDuration(trip, candidate) <= maxDuration;

				if (canCatchCandidate && notVisitedDestinationYet && tripTotalDurationNotExceeded) {
					newTrips.push({
						currentStop: candidate.destination,
						current: candidate.arrivalDt,
						visitedStops: [...trip.visitedStops, ...candidate.intermediaryStops],
						legs: [...trip.legs, edgeToTripLeg(candidate)]
					});
				}
			}
		}
	}

	if (newTrips.length > 0) {
		return [...trips, ...findTrips(newTrips, graph, maxLegs, maxDuration)];
	} else {
		return trips;
	}
};

const expectedDuration = (trip: InternalTrip, candidateLeg: Edge): number => {
	return candidateLeg.arrivalDt - (trip.legs.at(0)?.departureDt || candidateLeg.departureDt);
};

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
			if (trip.duration < existingTrip.trip.duration) {
				bestTrips.set(trip.destination, { node: nodes.get(trip.destination)!, trip });
			}
		}
	}

	return [...bestTrips.values()].sort((a, b) => (a.trip.duration > b.trip.duration ? 1 : -1));
};

import type { LineSchedule } from './api/schedule';

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
	arrival: string;
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
					arrival: destination.date_time,
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

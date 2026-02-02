import type { LineSchedule, ScheduleStop } from './api/schedule';

export interface Edge {
	origin: string;
	destination: string;
	departure: string;
	arrival: string;
	intermediaryStops: string[];
}

export const splitScheduleIntoNodeAndEdges = (
	schedule: LineSchedule
): { nodes: Map<string, ScheduleStop>; edges: Edge[] } => {
	const nodes = new Map();
	const edges = [];

	for (const trip of schedule) {
		const stops = trip.stops.toSorted((a, b) => (a.date_time > b.date_time ? 1 : -1));
		for (const [originIdx, origin] of stops.entries()) {
			nodes.set(origin.id, origin);
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
					intermediaryStops
				});
			}
		}
	}

	return { nodes, edges };
};

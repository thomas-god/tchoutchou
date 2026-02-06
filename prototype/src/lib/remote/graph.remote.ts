import { query } from '$app/server';
import type { Node } from '$lib/schedule';
import { findDestinations, getGraph } from '$lib/server/graph';
import dayjs from 'dayjs';

import z from 'zod';

export const fetchDestinationsQuery = query(
	z.object({
		origin: z.string(),
		from: z.string(),
		filters: z.object({
			maxConnections: z.number(),
			maxDuration: z.number(),
			minDuration: z.number()
		})
	}),
	async ({ origin, from, filters }) => {
		return await findDestinations(origin, dayjs(from), filters);
	}
);

export const fetchNodesQuery = query(z.object({ from: z.string() }), async ({ from }) => {
	const nodes: Node[] = [];
	const graph = await getGraph(dayjs(from));

	for (const [_, node] of graph.nodes.entries()) {
		nodes.push(node);
	}

	return nodes;
});

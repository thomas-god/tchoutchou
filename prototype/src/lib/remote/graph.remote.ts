import { query } from '$app/server';
import { findDestinations } from '$lib/server/graph';
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

import { query } from '$app/server';
import { findDestinations } from '$lib/server/graph';
import dayjs from 'dayjs';

import z from 'zod';

export const fetchDestinationsQuery = query(
	z.object({ origin: z.string(), from: z.string(), maxConnections: z.number() }),
	async ({ origin, from, maxConnections }) => {
		return await findDestinations(origin, dayjs(from), maxConnections);
	}
);

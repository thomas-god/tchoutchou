import { query } from '$app/server';
import { findTrips } from '$lib/server/graph';
import dayjs from 'dayjs';

import z from 'zod';

export const fetchTripsQuery = query(
	z.object({ origin: z.string(), from: z.string() }),
	async ({ origin, from }) => {
		return await findTrips(origin, dayjs(from));
	}
);

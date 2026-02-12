import { command } from '$app/server';
import { upsertZone, zoneCategories } from '$lib/server/destinations';
import z from 'zod';

const schema = z.object({
	category: z.enum(zoneCategories),
	name: z.string(),
	coordinates: z.array(
		z.object({
			lat: z.number(),
			lng: z.number()
		})
	)
});

export const insertZone = command(schema, async (zone) => {
	upsertZone(zone);
});

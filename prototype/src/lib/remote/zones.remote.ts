import { command, query } from '$app/server';
import { getZones, upsertZone, deleteZone, zoneCategories, type Zone } from '$lib/server/destinations';
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

const deleteSchema = z.object({
	category: z.enum(zoneCategories),
	name: z.string()
});

export const fetchZones = query(async (): Promise<Zone[]> => {
	return getZones();
});

export const insertZone = command(schema, async (zone) => {
	upsertZone(zone);
});

export const removeZone = command(deleteSchema, async ({ category, name }) => {
	deleteZone(category, name);
});

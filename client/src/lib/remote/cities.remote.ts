import { getEnv } from '$lib/env';
import { command, query } from '$app/server';
import z from 'zod';

const labelSchema = z.object({
	id: z.number(),
	name: z.string()
});

const cityWithExtraInformationSchema = z.object({
	id: z.number(),
	name: z.string(),
	country: z.string(),
	lat: z.number(),
	lon: z.number(),
	parent: z.number().nullable(),
	wikidata: z.string().nullable(),
	wikipedia: z.string().nullable(),
	labels: z.array(labelSchema)
});

export type CityWithExtraInformation = z.infer<typeof cityWithExtraInformationSchema>;
export type CityLabel = z.infer<typeof labelSchema>;

const citiesResponseSchema = z.object({
	cities: z.array(cityWithExtraInformationSchema)
});

export const fetchCities = query(z.undefined(), async (): Promise<CityWithExtraInformation[]> => {
	const url = getEnv('BACKEND_URL');
	const res = await fetch(`${url}/api/cities`);
	const data = await res.json();
	return citiesResponseSchema.parse(data).cities;
});

const setCityParentParamsSchema = z.object({
	cityId: z.number(),
	parentId: z.number().nullable()
});

export const setCityParent = command(
	setCityParentParamsSchema,
	async ({ cityId, parentId }: { cityId: number; parentId: number | null }): Promise<void> => {
		const url = getEnv('BACKEND_URL');
		const res = await fetch(`${url}/api/cities/${cityId}/parent`, {
			method: 'PUT',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ parent_id: parentId })
		});
		if (res.status === 404) throw new Error('City not found.');
		if (res.status === 422) throw new Error('Invalid parent city.');
		if (!res.ok) throw new Error('Failed to set parent.');
	}
);

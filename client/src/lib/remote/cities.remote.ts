import { getEnv } from '$lib/env';
import { query } from '$app/server';
import z from 'zod';

const cityWithExtraInformationSchema = z.object({
	id: z.number(),
	name: z.string(),
	country: z.string(),
	lat: z.number(),
	lon: z.number(),
	wikidata: z.string().nullable(),
	wikipedia: z.string().nullable()
});

export type CityWithExtraInformation = z.infer<typeof cityWithExtraInformationSchema>;

const citiesResponseSchema = z.object({
	cities: z.array(cityWithExtraInformationSchema)
});

export const fetchCities = query(z.undefined(), async (): Promise<CityWithExtraInformation[]> => {
	const url = getEnv('BACKEND_URL');
	const res = await fetch(`${url}/api/cities`);
	const data = await res.json();
	return citiesResponseSchema.parse(data).cities;
});

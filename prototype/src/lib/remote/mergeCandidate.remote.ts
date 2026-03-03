import { getEnv } from '$lib/env';
import { query } from '$app/server';
import z from 'zod';

const importedStationRefSchema = z.object({
	source: z.string(),
	source_id: z.string(),
	name: z.string()
});

const mergeCandidateItemSchema = z.object({
	id: z.number(),
	name: z.string(),
	lat: z.number(),
	lon: z.number(),
	distance_km: z.number(),
	sources: z.array(importedStationRefSchema)
});

const mergeCandidateGroupSchema = z.object({
	id: z.number(),
	name: z.string(),
	lat: z.number(),
	lon: z.number(),
	sources: z.array(importedStationRefSchema),
	candidates: z.array(mergeCandidateItemSchema)
});

const mergeCandidatesResponseSchema = z.object({
	stations: z.array(mergeCandidateGroupSchema)
});

export type ImportedStationRef = z.infer<typeof importedStationRefSchema>;
export type MergeCandidateItem = z.infer<typeof mergeCandidateItemSchema>;
export type MergeCandidateGroup = z.infer<typeof mergeCandidateGroupSchema>;

export const fetchMergeCandidates = query(
	z.object({ maxDistanceKm: z.number() }),
	async ({ maxDistanceKm }): Promise<MergeCandidateGroup[]> => {
		const backendUrl = getEnv('BACKEND_URL');
    console.log(backendUrl)
		const res = await fetch(
			`${backendUrl}/api/stations/nearby?max_distance_km=${maxDistanceKm}`
		);
		if (!res.ok) {
			throw new Error(`Failed to fetch merge candidates: ${res.status}`);
		}
		const data = await res.json();
		return mergeCandidatesResponseSchema.parse(data).stations;
	}
);

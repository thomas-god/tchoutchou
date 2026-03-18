import { getEnv } from '$lib/env';
import { command, query } from '$app/server';
import z from 'zod';

const labelSchema = z.object({
	id: z.number(),
	name: z.string()
});

export type Label = z.infer<typeof labelSchema>;

const labelsResponseSchema = z.object({
	labels: z.array(labelSchema)
});

export const fetchLabels = query(z.undefined(), async (): Promise<Label[]> => {
	const url = getEnv('BACKEND_URL');
	const res = await fetch(`${url}/api/labels`);
	const data = await res.json();
	return labelsResponseSchema.parse(data).labels;
});

export const createLabel = command(z.string(), async (name: string): Promise<Label> => {
	const url = getEnv('BACKEND_URL');
	const res = await fetch(`${url}/api/labels`, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ name })
	});
	if (res.status === 409) throw new Error('A label with this name already exists.');
	if (!res.ok) throw new Error('Failed to create label.');
	const data = await res.json();
	return { id: data.id, name };
});

const removeLabelFromCityParamsSchema = z.object({
	cityId: z.number(),
	labelId: z.number()
});

export const removeLabelFromCity = command(
	removeLabelFromCityParamsSchema,
	async ({ cityId, labelId }: { cityId: number; labelId: number }): Promise<void> => {
		const url = getEnv('BACKEND_URL');
		const res = await fetch(`${url}/api/cities/${cityId}/labels/${labelId}`, {
			method: 'DELETE'
		});
		if (res.status === 404) throw new Error('City or label not found.');
		if (!res.ok) throw new Error('Failed to remove label.');
	}
);

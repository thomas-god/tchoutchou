import { fetchMergeCandidates } from '$lib/remote/mergeCandidate.remote';

export const prerender = false;

export const load = () => {
	return { groups: fetchMergeCandidates({ maxDistanceKm: 1.0 }) };
};

import dayjs from 'dayjs';
import type { PageServerLoad } from './$types';
import { fetchJourney } from '$lib/journey';

const cdgId = 'stop_point:SNCF:87271494:LongDistanceTrain';
const partDieuId = 'stop_point:SNCF:87723197:LongDistanceTrain';

export const load: PageServerLoad = async () => {
	const date = dayjs().add(1, 'day');

	return await fetchJourney(partDieuId, cdgId, date);
};

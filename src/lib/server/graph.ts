import { getEdgesAndNodes, type Graph } from '$lib/api/schedule';
import type dayjs from 'dayjs';

const graphs: Map<string, Graph> = new Map();

export const getGraph = async (date: dayjs.Dayjs): Promise<Graph> => {
	let _date = date.format('YYYYMMDD');
	if (!graphs.has(_date)) {
		console.log(`Graph for date ${_date} not loaded in memory, fetching it...`);
		const graph = await getEdgesAndNodes(date);
		graphs.set(_date, graph);
	}
	return graphs.get(_date)!;
};

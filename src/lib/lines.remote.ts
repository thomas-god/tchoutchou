import { query } from '$app/server';
import { getEnv } from '$lib/env';

export interface Line {
	id: string;
	name: string;
	direction: string;
}

export const fetchLines = query(async (): Promise<Line[]> => {
	const mode = encodeURI('physical_mode:LongDistanceTrain');
	const pagination = {
		count: 200,
		page: 0
	};
	let url = `https://api.navitia.io/v1/coverage/sncf/physical_modes/${mode}/routes?disable_disruption=true&disable_geojson=true&count=${pagination.count}&start_page=${pagination.page}`;

	const lines = [];

	let i = 0;
	while (i < 3) {
		const res = await fetch(url, {
			headers: {
				Authorization: getEnv('VITE_API_KEY')
			}
		});
		const data = await res.json();

		for (const route of data.routes) {
			lines.push({
				name: route.name,
				direction: route.direction_type,
				id: route.id
			});
		}

		const next_page_link = data.links.find((link: any) => link.type === 'next');
		if (next_page_link === undefined) {
			break;
		} else {
			url = data.links.find((link: any) => link.type === 'next').href;
		}

		i++;
	}

	return lines;
});

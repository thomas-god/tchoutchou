import { env } from '$env/dynamic/private';
import { displayDuration } from '$lib';
import dayjs from 'dayjs';
import z from 'zod';
import { query } from '$app/server';

export interface Section {
	duration: string;
	mode: string;
	from: string;
	to: string;
	routeId: string;
}

export interface Journey {
	duration: string;
	transfers: number;
	sections: Section[];
}

const schema = z.object({
	from: z.string(),
	to: z.string(),
	date: z.date()
});

export const fetchJourneys = query(schema, async ({ from, to, date }): Promise<Journey[]> => {
	const dateUri = encodeURI(dayjs(date).toISOString());
	const res = await fetch(
		`https://api.navitia.io/v1/coverage/sncf/journeys?from=${encodeURI(from)}&to=${encodeURI(to)}&datetime=${dateUri}`,
		{
			headers: {
				Authorization: env.VITE_API_KEY
			}
		}
	);
	const data = await res.json();
	const journeys: Journey[] = data.journeys.map(extractJourney);
	return journeys;
});

const extractJourney = (journey: any): Journey => {
	const sections = extractSections(journey);
	return {
		duration: displayDuration(journey.duration),
		transfers: journey.nb_transfers,
		sections
	};
};

const extractSections = (journey: any): Section[] => {
	const sections = [];
	for (const section of journey.sections) {
		if (
			section.from === undefined ||
			section.to === undefined ||
			section.display_informations === undefined
		) {
			continue;
		}

		sections.push({
			duration: displayDuration(section.duration),
			mode: section.display_informations.physical_mode,
			from: section.from.name,
			to: section.to.name,
			routeId: section.links.find((elem: any) => elem.type === 'route').id
		});
	}

	return sections;
};

import { VITE_API_KEY } from '$env/static/private';
import { displayDuration } from '$lib';
import type dayjs from 'dayjs';

export interface Section {
	duration: string;
	mode: string;
	from: string;
	to: string;
}

export interface Journey {
	duration: string;
	transfers: number;
	sections: Section[];
}

export const fetchJourney = async (fromId: string, toId: string, date: dayjs.Dayjs) => {
	const dateUri = encodeURI(date.toISOString());
	const res = await fetch(
		`https://api.navitia.io/v1/coverage/sncf/journeys?from=${encodeURI(fromId)}&to=${encodeURI(toId)}&datetime=${dateUri}`,
		{
			headers: {
				Authorization: VITE_API_KEY
			}
		}
	);
	const data = await res.json();
	const journeys: Journey[] = data.journeys.map(extractJourney);
	return { journeys };
};

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
			to: section.to.name
		});
	}

	return sections;
};

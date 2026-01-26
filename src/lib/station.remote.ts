import { query } from '$app/server';
import { VITE_API_KEY } from '$env/static/private';
import * as z from 'zod'

export interface Station {
	id: string;
	name: string;
	lon: number;
	lat: number;
}

export const autocompleteStation = query(z.string(), async (query: string): Promise<Station[]> => {
	const res = await fetch(
		`https://api.navitia.io/v1/coverage/sncf/pt_objects?type[]=stop_point&q=${query}`,
		{
			headers: {
				Authorization: VITE_API_KEY
			}
		}
	);
	const data = await res.json();
	const stations = removeDuplicateStations(data.pt_objects.map((point: any) => ({
		id: point.id,
		name: point.name,
		lon: point.stop_point.coord.lon,
		lat: point.stop_point.coord.lat
	})) as Station[]);
  return stations
});

const removeDuplicateStations = (stations: Station[]): Station[] => {
  const res = [];
  const stationIds: string[] = [];
  for (const station of stations) {
    if (stationIds.includes(station.id)) {
      continue
    }
    if (!station.id.includes("LongDistanceTrain")) {
      continue
    }
    stationIds.push(station.id)
    res.push(station)
  }
  return res
}
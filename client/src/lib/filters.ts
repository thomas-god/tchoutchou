import type { DestinationResult } from './remote/destinations.remote';

export interface Range {
	min: number;
	max: number;
}

export const CONNECTIONS = [0, 1, 2] as const;
export type MaxConnections = (typeof CONNECTIONS)[number];

export interface DestinationFilters {
	duration: Range;
	maxConnections: MaxConnections;
}

/**
 * Applies filters to destinations (duration, max connections and labels)
 */
export function filterDestinations(
	destinations: DestinationResult[],
	filters: DestinationFilters,
	selectedLabels: number[] = []
): DestinationResult[] {
	return destinations.filter((d) => {
		// Check duration filter
		if (d.duration < filters.duration.min || d.duration > filters.duration.max) {
			return false;
		}

		// Check max connections filter (if specified)
		if (d.connections > filters.maxConnections) {
			return false;
		}

		// Check label filter
		if (selectedLabels.length > 0 && !d.city.labels.some((l) => selectedLabels.includes(l.id))) {
			return false;
		}

		return true;
	});
}

/**
 * Sorts destinations by duration (ascending)
 */
export function sortDestinationsByDuration(destinations: DestinationResult[]): DestinationResult[] {
	return [...destinations].sort((a, b) => a.duration - b.duration);
}

/**
 * Filters and sorts destinations by duration, connections and labels
 */
export function filterAndSortDestinations(
	destinations: DestinationResult[],
	filters: DestinationFilters,
	selectedLabels: number[] = []
): DestinationResult[] {
	return sortDestinationsByDuration(filterDestinations(destinations, filters, selectedLabels));
}

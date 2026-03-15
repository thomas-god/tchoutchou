import { describe, it, expect } from 'vitest';
import {
	filterDestinations,
	sortDestinationsByDuration,
	filterAndSortDestinations,
	type DestinationFilters
} from './filters';
import type { DestinationResult, City } from './remote/destinations.remote';

// Helper to create mock destinations
function createDestination(id: number, duration: number, connections = 0): DestinationResult {
	const city: City = {
		id,
		name: `City ${id}`,
		country: 'FR',
		lat: 48.8566 + id * 0.1,
		lon: 2.3522 + id * 0.1
	};

	return {
		station: city,
		duration,
		connections,
		visitedStations: [city]
	};
}

describe('filterDestinations', () => {
	it('should filter destinations by duration range', () => {
		const destinations = [
			createDestination(1, 1000),
			createDestination(2, 2000),
			createDestination(3, 3000),
			createDestination(4, 4000)
		];

		const filters: DestinationFilters = {
			duration: { min: 1500, max: 3500 },
			maxConnections: 2
		};

		const result = filterDestinations(destinations, filters);

		expect(result).toHaveLength(2);
		expect(result[0].duration).toBe(2000);
		expect(result[1].duration).toBe(3000);
	});

	it('should return empty array when no destinations match', () => {
		const destinations = [createDestination(1, 1000), createDestination(2, 2000)];

		const filters: DestinationFilters = {
			duration: { min: 5000, max: 10000 },
			maxConnections: 2
		};

		const result = filterDestinations(destinations, filters);

		expect(result).toHaveLength(0);
	});

	it('should include destinations at exact min/max boundaries', () => {
		const destinations = [
			createDestination(1, 1000),
			createDestination(2, 2000),
			createDestination(3, 3000)
		];

		const filters: DestinationFilters = {
			duration: { min: 1000, max: 3000 },
			maxConnections: 2
		};

		const result = filterDestinations(destinations, filters);

		expect(result).toHaveLength(3);
	});

	it('should return all destinations when range covers all', () => {
		const destinations = [
			createDestination(1, 1000),
			createDestination(2, 2000),
			createDestination(3, 3000)
		];

		const filters: DestinationFilters = {
			duration: { min: 0, max: 10000 },
			maxConnections: 2
		};

		const result = filterDestinations(destinations, filters);

		expect(result).toHaveLength(3);
	});

	it('should filter by maxConnections', () => {
		const destinations = [
			createDestination(1, 1000, 0),
			createDestination(2, 2000, 1),
			createDestination(3, 3000, 2),
			createDestination(4, 4000, 2)
		];

		const filters: DestinationFilters = {
			duration: { min: 0, max: 10000 },
			maxConnections: 1
		};

		const result = filterDestinations(destinations, filters);

		expect(result).toHaveLength(2);
		expect(result[0].connections).toBe(0);
		expect(result[1].connections).toBe(1);
	});

	it('should filter only direct connections when maxConnections is 0', () => {
		const destinations = [
			createDestination(1, 1000, 0),
			createDestination(2, 2000, 0),
			createDestination(3, 3000, 1),
			createDestination(4, 4000, 2)
		];

		const filters: DestinationFilters = {
			duration: { min: 0, max: 10000 },
			maxConnections: 0
		};

		const result = filterDestinations(destinations, filters);

		expect(result).toHaveLength(2);
		expect(result.every((d) => d.connections === 0)).toBe(true);
	});

	it('should combine duration and maxConnections filters', () => {
		const destinations = [
			createDestination(1, 1000, 0),
			createDestination(2, 2000, 1),
			createDestination(3, 3000, 2),
			createDestination(4, 4000, 1)
		];

		const filters: DestinationFilters = {
			duration: { min: 1500, max: 3500 },
			maxConnections: 1
		};

		const result = filterDestinations(destinations, filters);

		expect(result).toHaveLength(1);
		expect(result[0].duration).toBe(2000);
		expect(result[0].connections).toBe(1);
	});
});

describe('sortDestinationsByDuration', () => {
	it('should sort destinations by duration in ascending order', () => {
		const destinations = [
			createDestination(1, 3000),
			createDestination(2, 1000),
			createDestination(3, 2000)
		];

		const result = sortDestinationsByDuration(destinations);

		expect(result).toHaveLength(3);
		expect(result[0].duration).toBe(1000);
		expect(result[1].duration).toBe(2000);
		expect(result[2].duration).toBe(3000);
	});

	it('should not mutate original array', () => {
		const destinations = [createDestination(1, 3000), createDestination(2, 1000)];

		const original = [...destinations];
		sortDestinationsByDuration(destinations);

		expect(destinations).toEqual(original);
	});

	it('should handle empty array', () => {
		const result = sortDestinationsByDuration([]);

		expect(result).toHaveLength(0);
	});

	it('should handle single destination', () => {
		const destinations = [createDestination(1, 1000)];

		const result = sortDestinationsByDuration(destinations);

		expect(result).toHaveLength(1);
		expect(result[0].duration).toBe(1000);
	});
});

describe('filterAndSortDestinations', () => {
	it('should filter and sort destinations', () => {
		const destinations = [
			createDestination(1, 4000),
			createDestination(2, 2000),
			createDestination(3, 3000),
			createDestination(4, 1000)
		];

		const filters: DestinationFilters = {
			duration: { min: 1500, max: 3500 },
			maxConnections: 1
		};

		const result = filterAndSortDestinations(destinations, filters);

		expect(result).toHaveLength(2);
		expect(result[0].duration).toBe(2000);
		expect(result[1].duration).toBe(3000);
	});

	it('should return empty sorted array when no matches', () => {
		const destinations = [createDestination(1, 1000)];

		const filters: DestinationFilters = {
			duration: { min: 1500, max: 3500 },
			maxConnections: 1
		};

		const result = filterAndSortDestinations(destinations, filters);

		expect(result).toHaveLength(0);
	});

	it('should filter and sort with maxConnections', () => {
		const destinations = [
			createDestination(1, 4000, 2),
			createDestination(2, 2000, 1),
			createDestination(3, 3000, 0),
			createDestination(4, 1000, 1),
			createDestination(5, 5000, 2)
		];

		const filters: DestinationFilters = {
			duration: { min: 0, max: 10000 },
			maxConnections: 1
		};

		const result = filterAndSortDestinations(destinations, filters);

		expect(result).toHaveLength(3);
		expect(result[0].duration).toBe(1000);
		expect(result[0].connections).toBe(1);
		expect(result[1].duration).toBe(2000);
		expect(result[1].connections).toBe(1);
		expect(result[2].duration).toBe(3000);
		expect(result[2].connections).toBe(0);
	});
});

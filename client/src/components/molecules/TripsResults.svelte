<script lang="ts">
	import { fade } from 'svelte/transition';
	import { displayDuration } from '$lib';
	import type { Trip } from '$lib/server/graph';
	import TripsMap from './TripsMap.svelte';
	import type { EnrichedNode } from '$lib/server/destinations';
	import { type ZoneCategory } from '$lib/server/destinations';
	import type { Station } from '$lib/remote/station.remote';
	import DoubleRange from '../atoms/DoubleRange.svelte';
	import { SvelteSet } from 'svelte/reactivity';

	export interface Destination {
		node: EnrichedNode;
		trip: Trip;
	}

	let { origin, destinations }: { origin: Station; destinations: Destination[] } = $props();

	let selectedDestination: undefined | Destination = $state(undefined);

	let filteredDestinations = $derived(
		destinations.filter(
			(dest) =>
				filterPopulation(dest.node.population) &&
				filterMuseums(dest.node.numberOfMuseums) &&
				filterZones(dest.node.zones)
		)
	);

	export const zoneCategories = ['sea', 'mountain'] as const;

	const filterPopulation = (pop: number | null): boolean => {
		return pop === null
			? true
			: pop >= populationRange.min &&
					(populationRange.max === maxPop ? true : pop <= populationRange.max);
	};
	const filterMuseums = (nbOfMuseums: number | null): boolean => {
		return nbOfMuseums === null
			? true
			: nbOfMuseums >= museumsRange.min &&
					(museumsRange.max === maxMuseums ? true : nbOfMuseums <= museumsRange.max);
	};
	const filterZones = (zones: EnrichedNode['zones']): boolean => {
		if (selectedZones.size === 0) {
			return true;
		}
		if (!zones || zones.length === 0) {
			return false;
		}
		return zones.some((zone) => selectedZones.has(zone.category as ZoneCategory));
	};

	let bounds = $derived({
		lat: {
			min: Math.min(...destinations.map((destination) => destination.node.lat)),
			max: Math.max(...destinations.map((destination) => destination.node.lat))
		},
		lon: {
			min: Math.min(...destinations.map((destination) => destination.node.lon)),
			max: Math.max(...destinations.map((destination) => destination.node.lon))
		}
	});

	const maxPop = 5e5;
	let populationRange = $state({ min: 0, max: maxPop });
	const maxMuseums = 10;
	let museumsRange = $state({ min: 0, max: maxMuseums });
	let selectedZones = $state<SvelteSet<ZoneCategory>>(new SvelteSet());

	const toggleZone = (category: ZoneCategory) => {
		if (selectedZones.has(category)) {
			selectedZones.delete(category);
		} else {
			selectedZones.add(category);
		}
	};
</script>

<div class="@container flex flex-col gap-3 bg-base-300 p-3">
	<h2 class="text-sm font-semibold">
		{destinations.length} destinations trouvées, {filteredDestinations.length} correspondent à vos filtres
	</h2>
	<div class="flex flex-row gap-3">
		<div class="grow">
			<h3>Nombre d'habitants</h3>
			<div>
				<DoubleRange
					step={25000}
					range={{ min: 0, max: maxPop }}
					bind:selection={populationRange}
					fmt={(val) => `${val.toLocaleString('fr-FR')}${val === maxPop ? '+' : ''}`}
				/>
			</div>
		</div>
		<div class="grow">
			<h3>Nombre de musées</h3>
			<div>
				<DoubleRange
					step={1}
					range={{ min: 0, max: maxMuseums }}
					bind:selection={museumsRange}
					fmt={(val) => `${val.toLocaleString('fr-FR')}${val === maxMuseums ? '+' : ''}`}
				/>
			</div>
		</div>
	</div>
	<div>
		<h3>Zones</h3>
		<div class="flex gap-2">
			{#each zoneCategories as category}
				<button
					class="btn btn-sm {selectedZones.has(category) ? 'btn-primary' : 'btn-outline'}"
					onclick={() => toggleZone(category)}
				>
					{category === 'sea' ? '🌊 Mer' : '⛰️ Montagne'}
				</button>
			{/each}
		</div>
	</div>
	<div class="flex flex-col-reverse gap-3 @min-[500px]:max-h-112 @min-[500px]:flex-row">
		<div class="overflow-scroll @max-[500px]:h-96">
			{#each filteredDestinations as destination (destination.node.id)}
				<div class="p-1 hover:bg-base-100" in:fade|global out:fade|global={{ duration: 50 }}>
					<button onclick={() => (selectedDestination = destination)} class="w-full text-start">
						<h3 class="text-md font-semibold">{destination.node.name}</h3>
						<p class="text-xs italic">
							{displayDuration(destination.trip.duration)}
							{#if destination.node.population}
								<span>
									. {destination.node.population.toLocaleString('fr-FR')} hab
								</span>
							{/if}
							{#if destination.node.numberOfMuseums}
								<span>
									. {destination.node.numberOfMuseums.toLocaleString('fr-FR')} musée(s)
								</span>
							{/if}
						</p>
					</button>
				</div>
			{:else}
				<p class="text-warning">Pas de destination trouvée pour cette gare</p>
			{/each}
		</div>
		<div class="max-h-112 min-h-112 @max-[500px]:h-96 @min-[500px]:w-full">
			<TripsMap
				origin={{ lat: origin.lat, lon: origin.lon }}
				destinations={filteredDestinations}
				{selectedDestination}
				{bounds}
			/>
		</div>
	</div>
</div>

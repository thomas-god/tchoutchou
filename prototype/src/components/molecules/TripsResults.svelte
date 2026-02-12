<script lang="ts">
	import { fade } from 'svelte/transition';
	import { displayDuration } from '$lib';
	import type { Trip } from '$lib/server/graph';
	import TripsMap from './TripsMap.svelte';
	import type { EnrichedNode } from '$lib/server/destinations';
	import type { Station } from '$lib/remote/station.remote';

	export interface Destination {
		node: EnrichedNode;
		trip: Trip;
	}

	let { origin, destinations }: { origin: Station; destinations: Destination[] } = $props();

	let selectedDestination: undefined | Destination = $state(undefined);

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
</script>

<div class="@container flex flex-col gap-3 bg-base-300 p-3">
	<h2 class="text-sm font-semibold">{destinations.length} destinations trouvées</h2>
	<div class="flex flex-col-reverse gap-3 @min-[500px]:max-h-112 @min-[500px]:flex-row">
		<div class="overflow-scroll @max-[500px]:h-96">
			{#each destinations as destination (destination.node.id)}
				<div class="p-1 hover:bg-base-100" in:fade|global out:fade|global={{ duration: 50 }}>
					<button onclick={() => (selectedDestination = destination)} class="w-full text-start">
						<h3 class="text-md font-semibold">{destination.node.name}</h3>
						<p class="text-xs italic">
							{displayDuration(destination.trip.duration)}
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
				{destinations}
				{selectedDestination}
				{bounds}
			/>
		</div>
	</div>
</div>

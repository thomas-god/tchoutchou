<script lang="ts">
	import { fade } from 'svelte/transition';
	import { displayDuration } from '$lib';
	import type {
		BackendDestinationResult,
		BackendStation
	} from '$lib/remote/backend-schedules.remote';
	import BackendTripsMap from './BackendTripsMap.svelte';

	let {
		origin,
		destinations
	}: { origin: BackendStation; destinations: BackendDestinationResult[] } = $props();

	let selectedDestination: undefined | BackendDestinationResult = $state(undefined);

	let sortedDestinations = $derived([...destinations].sort((a, b) => a.duration - b.duration));

	let bounds = $derived({
		lat: {
			min: Math.min(...destinations.map((d) => d.station.lat)),
			max: Math.max(...destinations.map((d) => d.station.lat))
		},
		lon: {
			min: Math.min(...destinations.map((d) => d.station.lon)),
			max: Math.max(...destinations.map((d) => d.station.lon))
		}
	});
</script>

<div class="@container flex flex-col gap-3 bg-base-300 p-3">
	<h2 class="text-sm font-semibold">
		{destinations.length} destinations trouvées
	</h2>
	<div class="flex flex-col-reverse gap-3 @min-[500px]:max-h-112 @min-[500px]:flex-row">
		<div class="overflow-scroll @max-[500px]:h-96">
			{#each sortedDestinations as destination (destination.station.id)}
				<div class="p-1 hover:bg-base-100" in:fade|global out:fade|global={{ duration: 50 }}>
					<button onclick={() => (selectedDestination = destination)} class="w-full text-start">
						<h3 class="text-md font-semibold">{destination.station.name}</h3>
						<p class="text-xs italic">
							{displayDuration(destination.duration)}
							<span
								>·
								{#if destination.connections > 0}
									{destination.connections} correspondance(s)
								{:else}
									direct
								{/if}
							</span>
						</p>
					</button>
				</div>
			{:else}
				<p class="text-warning">Pas de destination trouvée pour cette gare</p>
			{/each}
		</div>
		<div class="max-h-112 min-h-112 @max-[500px]:h-96 @min-[500px]:w-full">
			<BackendTripsMap {origin} {destinations} {selectedDestination} {bounds} />
		</div>
	</div>
</div>

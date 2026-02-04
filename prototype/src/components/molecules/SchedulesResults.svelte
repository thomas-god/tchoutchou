<script lang="ts">
	import type { Destination } from '$lib/api/stop_schedules';
	import { fade } from 'svelte/transition';
	import SchedulesMap from './SchedulesMap.svelte';
	import { displayDuration } from '$lib';
	import type { Station } from '$lib/station.remote';
	import DurationRange from '../atoms/DurationRange.svelte';

	let { origin, destinations }: { origin: Station; destinations: Destination[] } = $props();

	let selectedDestination: undefined | Destination = $state(undefined);

	let durationStep = 900; // 15min
	let maxDuration = $derived(Math.max(...destinations.map((dest) => dest.duration)));
	let durationRange = $derived({ min: 0, max: maxDuration });

	let filteredDestinations = $derived(
		destinations.filter(
			(dest) => dest.duration <= durationRange.max && dest.duration >= durationRange.min
		)
	);
	let bounds = $derived({
		lat: {
			min: Math.min(...destinations.map((destination) => destination.stop.lat)),
			max: Math.max(...destinations.map((destination) => destination.stop.lat))
		},
		lon: {
			min: Math.min(...destinations.map((destination) => destination.stop.lon)),
			max: Math.max(...destinations.map((destination) => destination.stop.lon))
		}
	});
</script>

<div class="@container flex flex-col gap-3 bg-base-300 p-3">
	<h2 class="text-sm font-semibold">{destinations.length} destinations trouvées</h2>
	<div class="max-w-80">
		<h3 class="text-md italic">Durée du trajet</h3>
		<DurationRange
			range={{ min: 0, max: maxDuration }}
			bind:selection={durationRange}
			step={durationStep}
		/>
	</div>
	<div class="flex flex-col-reverse gap-3 @min-[500px]:max-h-112 @min-[500px]:flex-row">
		<div class="overflow-scroll @max-[500px]:h-96">
			{#each filteredDestinations as destination (destination.stop.id)}
				<div class="p-1 hover:bg-base-100" in:fade|global out:fade|global={{ duration: 50 }}>
					<button onclick={() => (selectedDestination = destination)} class="w-full text-start">
						<h3 class="text-md font-semibold">{destination.stop.name}</h3>
						<p class="text-xs italic">
							Train <span class="font-semibold">{destination.schedule.headsign}</span>, {displayDuration(
								destination.duration
							)}
						</p>
					</button>
				</div>
			{:else}
				<p class="text-warning">Pas de destination trouvée pour cette gare</p>
			{/each}
		</div>
		<div class="max-h-112 min-h-112 @max-[500px]:h-96 @min-[500px]:w-full">
			<SchedulesMap
				origin={{ lat: origin.lat, lon: origin.lon }}
				destinations={filteredDestinations}
				{selectedDestination}
				{bounds}
			/>
		</div>
	</div>
</div>

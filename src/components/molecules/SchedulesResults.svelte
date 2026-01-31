<script lang="ts">
	import type { Destination } from '$lib/api/stop_schedules';
	import { fade } from 'svelte/transition';
	import SchedulesMap from './SchedulesMap.svelte';
	import { displayDuration } from '$lib';
	import type { Station } from '$lib/station.remote';

	let { origin, destinations }: { origin: Station; destinations: Destination[] } = $props();

	let selectedDestination: undefined | Destination = $state(undefined);
</script>

<div class="@container flex flex-col gap-3 bg-base-300 p-3">
	<h2 class="text-sm font-semibold">{destinations.length} destinations trouvées</h2>
	<div class="flex flex-col-reverse gap-3 @min-[500px]:max-h-112 @min-[500px]:flex-row">
		<div class="overflow-scroll @max-[500px]:h-96">
			{#each destinations as destination (destination.stop.id)}
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
		<div class="max-h-112 min-h-56 @max-[500px]:h-96 @min-[500px]:w-full">
			<SchedulesMap
				origin={{ lat: origin.lat, lon: origin.lon }}
				{destinations}
				{selectedDestination}
			/>
		</div>
	</div>
</div>

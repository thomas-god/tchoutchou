<script lang="ts">
	import { fetchJourneys, type Journey } from '$lib/journey.remote';
	import type { Station } from '$lib/station.remote';
	import StationSelect from '../components/molecules/StationSelect.svelte';

	let from: Station | undefined = $state(undefined);
	let to: Station | undefined = $state(undefined);

	let journeysPromise: Promise<Journey[]> | undefined = $state(undefined);

	$effect(() => {
		if (from !== undefined && to !== undefined) {
			journeysPromise = fetchJourneys({ from: from.id, to: to.id, date: new Date() });
		}
	});

	$inspect(from, to);
</script>

<div class="m-3 flex flex-row gap-3 bg-accent p-3 text-accent-content">
	<StationSelect bind:station={from} label={'Départ'} />
	<StationSelect bind:station={to} label={'Arrivée'} />
</div>

{#if journeysPromise !== undefined}
	{#await journeysPromise then journeys}
		{#each journeys as journey}
			<div class="m-3 bg-accent p-3 text-accent-content">
				<h2>
					{journey.duration} - {journey.transfers} transfers
				</h2>
				<ul class="px-3">
					{#each journey.sections as section}
						<li class="list-disc">
							{section.from} -> {section.to} ({section.duration} en {section.mode})
						</li>
					{/each}
				</ul>
			</div>
		{/each}
	{/await}
{/if}

<script lang="ts">
	import { fetchJourneys, type Journey } from '$lib/journey.remote';
	import { fetchLinesQuery } from '$lib/lines.remote';
	import type { Station } from '$lib/station.remote';
	import dayjs from 'dayjs';
	import Lines from '../components/molecules/Lines.svelte';
	import Schedule from '../components/molecules/Schedule.svelte';
	import StationSelect from '../components/molecules/StationSelect.svelte';

	let from: Station | undefined = $state(undefined);
	let to: Station | undefined = $state(undefined);

	let journeysPromise: Promise<Journey[]> | undefined = $state(undefined);

	let lines = await fetchLinesQuery();
	let selectedLine = $state(undefined);
	let rawDate = $state(dayjs().format('YYYY-MM-DD'));
	let fromDate = $derived(dayjs(rawDate).toDate());

	$effect(() => {
		if (from !== undefined && to !== undefined) {
			journeysPromise = fetchJourneys({ from: from.id, to: to.id, date: new Date() });
		}
	});
</script>

<div class="m-3 flex flex-row gap-3 bg-base-300 p-3">
	<StationSelect bind:station={from} label={'Départ'} />
	<StationSelect bind:station={to} label={'Arrivée'} />
</div>

{#if journeysPromise !== undefined}
	{#await journeysPromise then journeys}
		{#each journeys as journey}
			<div class="m-3 p-3">
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

<div class="m-3 flex flex-col gap-3 bg-base-300 p-3">
	<div class="flex flex-row items-end gap-3">
		<Lines {lines} bind:selectedLine bind:from={rawDate} />
	</div>
	{#if selectedLine !== undefined}
		<Schedule line={selectedLine} from={fromDate} />
	{/if}
</div>

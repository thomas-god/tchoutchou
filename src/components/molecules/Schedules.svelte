<script lang="ts">
	import dayjs from 'dayjs';
	import StationSelect from './StationSelect.svelte';
	import type { Station } from '$lib/station.remote';
	import { fetchDestinationsQuery } from '$lib/remote/graph.remote';
	import TripsResults from './TripsResults.svelte';

	const today = dayjs();
	const nextWeek = today.add(1, 'week');
	const format = (d: dayjs.Dayjs) => d.format('YYYY-MM-DD');

	let stop: Station | undefined = $state({
		id: 'stop_point:SNCF:87319012:LongDistanceTrain',
		name: 'Aix-en-Provence TGV (Aix-en-Provence)',
		lon: 5.317534,
		lat: 43.455237
	});
	let from: string = $state(format(today));
	let maxConnections = $state(1);

	let tripsPromise = $derived.by(() => {
		if (stop === undefined || from === undefined) {
			return undefined;
		}

		return fetchDestinationsQuery({ origin: stop.id, from, maxConnections });
	});
</script>

<div class="mx-auto flex max-w-4xl flex-col gap-3 p-3">
	<div class="bg-base-300 p-3">
		<fieldset class="fieldset">
			<StationSelect bind:station={stop} label={'Gare de départ'} />

			<div class="flex flex-col items-start gap-2">
				<label for="select-from-date" class="text-sm font-semibold">Date de départ </label>
				<input
					type="date"
					class="input pl-2"
					id="select-from-date"
					bind:value={from}
					min={format(today)}
					max={format(nextWeek)}
				/>
			</div>
			<div class="flex flex-col items-start gap-2">
				<p class="text-sm font-semibold">Nombre de correspondances</p>
				<div class="flex flex-row gap-2">
					{#each [0, 1, 2] as connection}
						<button
							class={`btn ${connection === maxConnections ? 'btn-primary' : ''}`}
							onclick={() => (maxConnections = connection)}>{connection}</button
						>
					{/each}
				</div>
			</div>
		</fieldset>
	</div>

	{#if tripsPromise !== undefined}
		{#await tripsPromise}
			<span class="loading loading-xl self-center loading-dots pt-3"></span>
		{:then destinations}
			<TripsResults {destinations} origin={stop!} />
		{/await}
	{/if}
</div>

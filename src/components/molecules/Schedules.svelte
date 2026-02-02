<script lang="ts">
	import dayjs from 'dayjs';
	import StationSelect from './StationSelect.svelte';
	import type { Station } from '$lib/station.remote';
	import { fetchDestinationsQuery } from '$lib/remote/graph.remote';
	import TripsResults from './TripsResults.svelte';
	import DurationRange from '../atoms/DurationRange.svelte';

	const today = dayjs();
	const nextWeek = today.add(1, 'week');
	const format = (d: dayjs.Dayjs) => d.format('YYYY-MM-DD');

	let stop: Station | undefined = $state(undefined);
	let from: string = $state(format(today));
	let maxConnections = $state(1);
	const maxDurationUpperBound = 24 * 3600;
	let durationRange = $derived({ min: 3600, max: 8 * 3600 });

	let tripsPromise = $derived.by(() => {
		if (stop === undefined || from === undefined) {
			return undefined;
		}

		return fetchDestinationsQuery({
			origin: stop.id,
			from,
			filters: { maxConnections, minDuration: durationRange.min, maxDuration: durationRange.max }
		});
	});
</script>

<div class="mx-auto flex max-w-4xl flex-col gap-3 p-3">
	<div class="bg-base-300 p-3">
		<fieldset class="fieldset">
			<StationSelect bind:station={stop} label={'Gare de départ'} />

			<div class="flex flex-col items-start justify-stretch gap-2">
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
			<div class="flex max-w-90 flex-col items-start gap-2">
				<p class="text-sm font-semibold">Durée du trajet</p>
				<div class="w-full">
					<DurationRange
						range={{ min: 0, max: maxDurationUpperBound }}
						bind:selection={durationRange}
						step={1800}
					/>
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

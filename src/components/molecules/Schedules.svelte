<script lang="ts">
	import dayjs from 'dayjs';
	import { fade } from 'svelte/transition';
	import StationSelect from './StationSelect.svelte';
	import { fetchStopSchedulesQuery } from '$lib/stop_schedules.remote';
	import type { Station } from '$lib/station.remote';
	import { displayDuration } from '$lib';

	const today = dayjs();
	const nextWeek = today.add(1, 'week');
	const format = (d: dayjs.Dayjs) => d.format('YYYY-MM-DD');

	let stop: Station | undefined = $state(undefined);
	let from: string = $state(format(today));

	let promise = $derived.by(() => {
		if (stop === undefined || from === undefined) {
			return undefined;
		}

		return fetchStopSchedulesQuery({ stop: stop.id, from });
	});
</script>

<div class="flex flex-col gap-3 bg-base-300 p-3">
	<fieldset class="fieldset">
		<legend class="fieldset-legend">Trajets au départ de</legend>
		<StationSelect bind:station={stop} label={'Gare de départ'} />

		<div class="flex flex-row items-center gap-3">
			<label for="select-from-date">Date de départ </label>
			<input
				type="date"
				class="input"
				id="select-from-date"
				bind:value={from}
				min={format(today)}
				max={format(nextWeek)}
			/>
		</div>
	</fieldset>

	{#if promise !== undefined}
		{#await promise}
			<span class="loading loading-xl self-center loading-dots pt-3"></span>
		{:then destinations}
			{#each destinations as destination (destination.stop.id)}
				<div class="p-2" in:fade|global out:fade|global={{ duration: 50 }}>
					<h3 class="text-lg font-semibold">{destination.stop.name}</h3>
					<p class="italic">
						TGV <span class="font-semibold">{destination.schedule.headsign}</span>, {displayDuration(
							destination.duration
						)}
					</p>
				</div>
			{:else}
				<p class="text-warning">Pas de destination trouvée pour cette gare</p>
			{/each}
		{/await}
	{/if}
</div>

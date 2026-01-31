<script lang="ts">
	import dayjs from 'dayjs';
	import StationSelect from './StationSelect.svelte';
	import { fetchStopSchedulesQuery } from '$lib/stop_schedules.remote';
	import type { Station } from '$lib/station.remote';
	import SchedulesResults from './SchedulesResults.svelte';

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
		</fieldset>
	</div>

	{#if promise !== undefined}
		{#await promise}
			<span class="loading loading-xl self-center loading-dots pt-3"></span>
		{:then destinations}
			<SchedulesResults {destinations} origin={stop!} />
		{/await}
	{/if}
</div>

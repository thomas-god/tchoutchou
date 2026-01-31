<script lang="ts">
	import dayjs from 'dayjs';
	import { fade } from 'svelte/transition';
	import StationSelect from './StationSelect.svelte';
	import { fetchStopSchedulesQuery } from '$lib/stop_schedules.remote';
	import type { Station } from '$lib/station.remote';
	import { displayDuration } from '$lib';
	import SchedulesMap from './SchedulesMap.svelte';

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
			<div class="@container flex flex-col gap-3 bg-base-300 p-3">
				<h2 class="text-sm font-semibold">{destinations.length} destinations trouvées</h2>
				<div class="flex flex-col-reverse gap-3 @min-[500px]:max-h-112 @min-[500px]:flex-row">
					<div class="overflow-scroll @max-[500px]:h-96">
						{#each destinations as destination (destination.stop.id)}
							<div class="" in:fade|global out:fade|global={{ duration: 50 }}>
								<h3 class="text-md font-semibold">{destination.stop.name}</h3>
								<p class="text-xs italic">
									Train <span class="font-semibold">{destination.schedule.headsign}</span>, {displayDuration(
										destination.duration
									)}
								</p>
							</div>
						{:else}
							<p class="text-warning">Pas de destination trouvée pour cette gare</p>
						{/each}
					</div>
					<div class="max-h-112 min-h-56 @max-[500px]:h-96 @min-[500px]:w-full">
						<SchedulesMap origin={{ lat: stop!.lat, lon: stop!.lon }} {destinations} />
					</div>
				</div>
			</div>
		{/await}
	{/if}
</div>

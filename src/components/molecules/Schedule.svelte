<script lang="ts">
	import type { Line } from '$lib/api/lines';
	import { fetchLineSchedule } from '$lib/line_schedule.remote';
	import dayjs from 'dayjs';
	import { fade } from 'svelte/transition';

	let { line, from }: { line: Line; from?: Date } = $props();

	let lineSchedulePromise = $derived(fetchLineSchedule({ line: line.id, from }));

	const localeTime = (date: dayjs.Dayjs): string => {
		return date.format('HH:mm');
	};
</script>

<div class="flex flex-col">
	{#await lineSchedulePromise}
		<span class="loading loading-xl self-center loading-dots pt-3"></span>
	{:then lineSchedule}
		{#each lineSchedule as schedule (schedule.id)}
			<div class="p-2" in:fade|global out:fade|global={{ duration: 50 }}>
				<h3 class="text-lg font-semibold">{schedule.route}</h3>
				<p class="italic">
					TGV <span class="font-semibold">{schedule.headsign}</span>, direction: {schedule.direction}
				</p>
				<p>Arrets</p>
				<ul class="ml-2">
					{#each schedule.stops as stop}
						<li>{stop.name}: {localeTime(dayjs(stop.date_time))}</li>
					{/each}
				</ul>
			</div>
		{:else}
			<p class="text-warning">Pas de programmation trouvée pour cette ligne</p>
		{/each}
	{/await}
</div>

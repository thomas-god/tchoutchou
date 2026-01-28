<script lang="ts">
	import { fetchLineSchedule } from '$lib/line_schedule.remote';
	import type { Line } from '$lib/lines.remote';
	import dayjs from 'dayjs';

	let { line }: { line: Line } = $props();

	let lineSchedule = $derived(await fetchLineSchedule({ line: line.id }));

	const localeTime = (date: dayjs.Dayjs): string => {
		return date.format('HH:mm');
	};
</script>

<div class="flex flex-col">
	{#each lineSchedule as schedule (schedule.id)}
		<div class="p-2">
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
</div>

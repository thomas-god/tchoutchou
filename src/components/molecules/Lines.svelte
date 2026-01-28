<script lang="ts">
	import type { Line } from '$lib/lines.remote';
	import dayjs from 'dayjs';

	let {
		lines,
		selectedLine = $bindable(),
		from = $bindable()
	}: { lines: Line[]; selectedLine: Line | undefined; from: string | undefined } = $props();
</script>

<div class="flex flex-col gap-2">
	<fieldset class="fieldset">
		<legend class="fieldset-legend">Programmation</legend>
		<label for="select-line">Lignes </label>
		<select class="select" id="select-line" bind:value={selectedLine}>
			<option disabled selected value={undefined}>Sélectionner une ligne de train</option>
			{#each lines as line (line.id)}
				<option value={line}>
					{line.name}
				</option>
			{/each}
		</select>

		<label for="select-from-date">Date de départ </label>
		<input
			type="date"
			class="input"
			id="select-from-date"
			bind:value={from}
			min={dayjs().format('YYYY-MM-DD')}
		/>
	</fieldset>
</div>

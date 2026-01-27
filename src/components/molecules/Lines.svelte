<script lang="ts">
	import type { Line } from '$lib/lines.remote';

	let { lines, selectedLine = $bindable() }: { lines: Line[]; selectedLine: Line | undefined } =
		$props();
</script>

<div class="flex flex-col gap-2">
	<fieldset class="fieldset">
		<legend class="fieldset-legend">Lignes</legend>
		<select class="select">
			<option disabled selected>Sélectionner une ligne de train</option>
			{#each lines as line (line.id)}
				<option value="line.name" onclick={() => (selectedLine = line)}>
					{line.name}
				</option>
			{/each}
		</select>
	</fieldset>
</div>

<script lang="ts">
	import type { Label } from '$lib/remote/labels.remote';

	interface Props {
		labels: Label[];
		selected: number[];
	}

	let { labels, selected = $bindable([]) }: Props = $props();

	function toggle(id: number) {
		if (selected.includes(id)) {
			selected = selected.filter((s) => s !== id);
		} else {
			selected = [...selected, id];
		}
	}
</script>

<div class="flex flex-wrap gap-2">
	{#each labels as label (label.id)}
		<button
			type="button"
			class="badge cursor-pointer badge-lg transition-colors select-none"
			class:badge-primary={selected.includes(label.id)}
			class:badge-base={!selected.includes(label.id)}
			onclick={() => toggle(label.id)}
		>
			{label.name}
		</button>
	{/each}
</div>

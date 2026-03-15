<script lang="ts">
	import DoubleRange from '../atoms/DoubleRange.svelte';
	import { displayDuration, type DestinationFilters, CONNECTIONS } from '$lib';

	let {
		filters = $bindable(),
		okCallback
	}: {
		filters: DestinationFilters;
		okCallback: () => void;
	} = $props();
</script>

<div class="flex flex-col gap-6">
	<!-- Duration Filter -->
	<div class="flex flex-col gap-3">
		<h3 class="text-lg font-semibold">Durée du trajet</h3>
		<DoubleRange
			range={{ min: 0, max: 24 * 3600 }}
			bind:selection={() => filters.duration, (v) => (filters = { ...filters, duration: v })}
			step={300}
			fmt={displayDuration}
		/>
	</div>

	<!-- Max Connections Filter -->
	<div class="flex flex-col gap-3">
		<h3 class="text-lg font-semibold">Nombre de correspondances max</h3>
		<div class="join">
			{#each CONNECTIONS as value (value)}
				<button
					type="button"
					class="btn join-item"
					class:btn-primary={filters.maxConnections === value}
					onclick={() => (filters = { ...filters, maxConnections: value })}
				>
					{value}
				</button>
			{/each}
		</div>
	</div>

	<div class="flex w-full flex-col">
		<button class="btn btn-accent" onclick={() => okCallback()}>Ok</button>
	</div>
</div>

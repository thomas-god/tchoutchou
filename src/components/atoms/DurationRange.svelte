<script lang="ts">
	import { displayDuration } from '$lib';

	export interface Range {
		min: number;
		max: number;
	}

	let {
		range,
		selection = $bindable(),
		step
	}: { range: Range; selection: Range; step: number } = $props();
	let min = $derived(Math.floor(range.min / step) * step);
	let max = $derived(Math.ceil(range.max / step) * step);

	function handleMinChange(e: Event) {
		const value = Number((e.target as HTMLInputElement).value);
		selection = { ...selection, min: Math.min(value, selection.max - step) };
	}

	function handleMaxChange(e: Event) {
		const value = Number((e.target as HTMLInputElement).value);
		selection = { ...selection, max: Math.max(value, selection.min + step) };
	}

	// Calculate the percentage position for styling
	let minPercent = $derived(((selection.min - range.min) / (max - range.min)) * 100);
	let maxPercent = $derived(((selection.max - range.min) / (max - range.min)) * 100);

	function handleTrackClick(e: MouseEvent) {
		const target = e.currentTarget as HTMLElement;
		const rect = target.getBoundingClientRect();
		const clickX = e.clientX - rect.left;
		const percentage = clickX / rect.width;
		const rawValue = min + percentage * (max - min);
		const clickValue = Math.round(rawValue / step) * step;

		// Determine which thumb is closer to the click
		const distToMin = Math.abs(clickValue - selection.min);
		const distToMax = Math.abs(clickValue - selection.max);

		if (distToMin < distToMax) {
			// Update min thumb, but don't let it exceed max
			selection = { ...selection, min: Math.min(clickValue, selection.max - step) };
		} else {
			// Update max thumb, but don't let it go below min
			selection = { ...selection, max: Math.max(clickValue, selection.min + step) };
		}
	}
</script>

<div class="flex flex-col gap-2">
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="relative h-8 cursor-pointer" onclick={handleTrackClick}>
		<!-- Track background -->
		<div class="absolute top-1/2 h-2 w-full -translate-y-1/2 rounded-full bg-base-200"></div>

		<!-- Active range highlight -->
		<div
			class="absolute top-1/2 h-2 -translate-y-1/2 rounded-full bg-primary"
			style="left: {minPercent}%; right: {100 - maxPercent}%"
		></div>

		<!-- Min range input -->
		<input
			type="range"
			{min}
			{max}
			value={selection.min}
			class="range-input absolute w-full"
			{step}
			oninput={handleMinChange}
		/>

		<!-- Max range input -->
		<input
			type="range"
			{min}
			{max}
			value={selection.max}
			class="range-input absolute w-full"
			{step}
			oninput={handleMaxChange}
		/>
	</div>

	<div class="flex justify-between text-sm">
		<span>{displayDuration(selection.min)}</span>
		<span>{displayDuration(selection.max)}</span>
	</div>
</div>

<style>
	.range-input {
		pointer-events: none;
		appearance: none;
		background: transparent;
		height: 2rem;
	}

	.range-input::-webkit-slider-thumb {
		pointer-events: all;
		-webkit-appearance: none;
		appearance: none;
		width: 1.25rem;
		height: 1.25rem;
		border-radius: 50%;
		background-color: var(--color-primary);
		border: 2px solid var(--color-base-100);
		cursor: pointer;
		box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
	}

	.range-input::-moz-range-thumb {
		pointer-events: all;
		appearance: none;
		width: 1.25rem;
		height: 1.25rem;
		border-radius: 50%;
		border: none;
		background-color: var(--color-primary);
		border: 2px solid var(--color-base-100);
		cursor: pointer;
		box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
	}

	.range-input::-webkit-slider-thumb:hover {
		transform: scale(1.1);
	}

	.range-input::-moz-range-thumb:hover {
		transform: scale(1.1);
	}
</style>

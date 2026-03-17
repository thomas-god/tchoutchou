<script lang="ts">
	import { fetchCities } from '$lib/remote/cities.remote';
	import { createLabel, fetchLabels, type Label } from '$lib/remote/labels.remote';
	import CitiyList from '../../components/pages/CitiyList.svelte';

	let labels: Label[] = $state([]);
	let labelsError = $state('');
	let newLabelName = $state('');
	let createError = $state('');
	let creating = $state(false);

	fetchLabels(undefined)
		.then((l) => (labels = l))
		.catch(() => (labelsError = 'Failed to load labels.'));

	async function handleCreate(e: SubmitEvent) {
		e.preventDefault();
		const name = newLabelName.trim();
		if (!name) return;
		creating = true;
		createError = '';
		try {
			const label = await createLabel(name);
			labels = [...labels, label];
			newLabelName = '';
		} catch (err) {
			createError = err instanceof Error ? err.message : 'Failed to create label.';
		} finally {
			creating = false;
		}
	}
</script>

<div class="flex h-screen flex-col overflow-hidden">
	<div class="border-b border-base-300 px-4 py-3">
		<h2 class="mb-2 text-lg font-semibold">Labels</h2>

		{#if labelsError}
			<p class="text-sm text-error">{labelsError}</p>
		{:else}
			<div class="flex flex-wrap items-center gap-2">
				{#each labels as label (label.id)}
					<span class="badge badge-neutral">{label.name}</span>
				{/each}

				<form class="flex items-center gap-2" onsubmit={handleCreate}>
					<input
						class="input-bordered input input-sm w-40"
						type="text"
						placeholder="New label…"
						bind:value={newLabelName}
						disabled={creating}
					/>
					<button
						class="btn btn-sm btn-primary"
						type="submit"
						disabled={creating || !newLabelName.trim()}
					>
						{#if creating}
							<span class="loading loading-xs loading-spinner"></span>
						{:else}
							Add
						{/if}
					</button>
					{#if createError}
						<p class="text-sm text-error">{createError}</p>
					{/if}
				</form>
			</div>
		{/if}
	</div>

	<h1 class="px-4 pt-4 text-2xl">Cities</h1>

	{#await fetchCities(undefined)}
		<p class="loading loading-dots">Loading…</p>
	{:then cities}
		<CitiyList {cities} />
	{:catch}
		<p class="error status">Failed to load cities.</p>
	{/await}
</div>

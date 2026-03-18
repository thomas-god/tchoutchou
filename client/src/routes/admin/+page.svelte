<script lang="ts">
	import {
		fetchCities,
		setCityParent,
		type CityWithExtraInformation
	} from '$lib/remote/cities.remote';
	import {
		addLabelToCity,
		createLabel,
		fetchLabels,
		removeLabelFromCity,
		type Label
	} from '$lib/remote/labels.remote';
	import CitiyList from '../../components/pages/CitiyList.svelte';

	let labels: Label[] = $state([]);
	let labelsError = $state('');
	let newLabelName = $state('');
	let createError = $state('');
	let creating = $state(false);
	let cities: CityWithExtraInformation[] = $state([]);

	fetchLabels(undefined)
		.then((l) => (labels = l))
		.catch(() => (labelsError = 'Failed to load labels.'));

	fetchCities(undefined).then((c) => (cities = c));

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

	async function handleRemoveLabel(cityId: number, labelId: number) {
		await removeLabelFromCity({ cityId, labelId });
		cities = cities.map((c) =>
			c.id === cityId ? { ...c, labels: c.labels.filter((l) => l.id !== labelId) } : c
		);
	}

	async function handleAddLabel(cityId: number, labelId: number) {
		await addLabelToCity({ cityId, labelId });
		const label = labels.find((l) => l.id === labelId);
		if (!label) return;
		cities = cities.map((c) =>
			c.id === cityId && !c.labels.some((l) => l.id === labelId)
				? { ...c, labels: [...c.labels, label] }
				: c
		);
	}

	async function handleSetParent(cityId: number, parentId: number | null) {
		await setCityParent({ cityId, parentId });
		cities = cities.map((c) => (c.id === cityId ? { ...c, parent: parentId } : c));
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

	{#if cities.length === 0}
		<p class="loading loading-dots">Loading…</p>
	{:else}
		<CitiyList
			{cities}
			onRemoveLabel={handleRemoveLabel}
			availableLabels={labels}
			onAddLabel={handleAddLabel}
			onSetParent={handleSetParent}
		/>
	{/if}
</div>

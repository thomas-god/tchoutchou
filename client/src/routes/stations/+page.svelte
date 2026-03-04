<script lang="ts">
	import { invalidateAll } from '$app/navigation';
	import {
		remapStation,
		type MergeCandidateGroup,
		type MergeCandidateItem
	} from '$lib/remote/mergeCandidate.remote';

	let { data } = $props();

	// key: `${groupId}-${candidateId}`, value: 'idle' | 'pending' | 'done' | 'error'
	let mergeState: Record<string, 'idle' | 'pending' | 'done' | 'error'> = $state({});

	// Confirmation dialog state
	let dialog: HTMLDialogElement | undefined = $state();
	let pendingMerge: { group: MergeCandidateGroup; candidate: MergeCandidateItem } | null =
		$state(null);

	function requestMerge(group: MergeCandidateGroup, candidate: MergeCandidateItem) {
		pendingMerge = { group, candidate };
		dialog?.showModal();
	}

	function cancelMerge() {
		dialog?.close();
		pendingMerge = null;
	}

	/**
	 * Merge `group` into `candidate`: remap every imported source station of
	 * `group` to the candidate's internal station id.
	 */
	async function confirmMerge() {
		if (!pendingMerge) return;
		const { group, candidate } = pendingMerge;
		dialog?.close();
		const key = `${group.id}-${candidate.id}`;
		mergeState[key] = 'pending';
		pendingMerge = null;
		try {
			for (const ref of group.sources) {
				await remapStation({
					source: ref.source,
					source_id: ref.source_id,
					internal_id: candidate.id
				});
			}
			mergeState = {};
			// Re-run the load function to refresh the list
			await invalidateAll();
		} catch {
			mergeState[key] = 'error';
		}
	}
</script>

<div class="mx-auto max-w-4xl p-6">
	<h1 class="mb-6 text-2xl font-bold">Station Merge Candidates</h1>

	{#await data.groups}
		<div class="flex justify-center py-12">
			<span class="loading loading-lg loading-spinner text-primary"></span>
		</div>
	{:then groups}
		{#if groups.length === 0}
			<p class="text-base-content/60">No stations with merge candidates within 1 km.</p>
		{:else}
			<p class="mb-4 text-sm text-base-content/60">
				{groups.length} station{groups.length !== 1 ? 's' : ''} with nearby candidates within 1 km
			</p>
			<div class="flex flex-col gap-2">
				{#each groups.toSorted((a, b) => a.name.localeCompare(b.name)) as group (group.id)}
					<div class="collapse-arrow collapse border border-base-300 bg-base-100">
						<input type="checkbox" />
						<div class="collapse-title flex items-center gap-3">
							<span class="font-semibold">{group.name}</span>
							<span class="text-xs text-base-content/50">#{group.id}</span>
							{#each group.sources as ref}
								<span class="badge badge-outline badge-sm">{ref.source}</span>
							{/each}
							<span class="ml-auto text-sm text-base-content/60">
								{group.candidates.length} candidate{group.candidates.length !== 1 ? 's' : ''}
								· nearest {group.candidates[0].distance_km.toFixed(2)} km
							</span>
						</div>
						<div class="collapse-content">
							<table class="table mt-2 w-full table-sm">
								<thead>
									<tr>
										<th>Name</th>
										<th>ID</th>
										<th>Sources</th>
										<th class="text-right">Distance</th>
										<th></th>
									</tr>
								</thead>
								<tbody>
									{#each group.candidates as candidate (candidate.id)}
										{@const key = `${group.id}-${candidate.id}`}
										{@const state = mergeState[key] ?? 'idle'}
										<tr>
											<td>{candidate.name}</td>
											<td class="text-xs text-base-content/50">#{candidate.id}</td>
											<td class="flex flex-wrap gap-1">
												{#each candidate.sources as ref}
													<span class="badge badge-outline badge-sm">{ref.source}</span>
												{/each}
											</td>
											<td class="text-right font-mono text-sm">
												{candidate.distance_km.toFixed(2)} km
											</td>
											<td class="text-right">
												{#if state === 'done'}
													<span class="text-xs text-success">Merged ✓</span>
												{:else if state === 'error'}
													<span class="text-xs text-error">Failed</span>
												{:else}
													<button
														class="btn btn-outline btn-xs"
														disabled={state === 'pending'}
														onclick={() => requestMerge(group, candidate)}
													>
														{#if state === 'pending'}
															<span class="loading loading-xs loading-spinner"></span>
														{:else}
															Merge into
														{/if}
													</button>
												{/if}
											</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{:catch err}
		<div class="alert alert-error">
			<span>Failed to load merge candidates: {err.message}</span>
		</div>
	{/await}
</div>

<!-- Confirmation dialog -->
<dialog bind:this={dialog} class="modal">
	<div class="modal-box">
		{#if pendingMerge}
			<h3 class="mb-4 text-lg font-bold">Confirm merge</h3>

			<p class="mb-3 text-sm text-base-content/70">
				The following imported station{pendingMerge.group.sources.length !== 1 ? 's' : ''} will be reassigned
				to <span class="font-semibold">{pendingMerge.candidate.name}</span>
				<span class="text-xs text-base-content/50">(#{pendingMerge.candidate.id})</span>:
			</p>

			<ul class="mb-4 space-y-1 rounded-lg bg-base-200 p-3 text-sm">
				{#each pendingMerge.group.sources as ref}
					<li class="flex items-center gap-2">
						<span class="badge badge-outline badge-sm">{ref.source}</span>
						<span class="font-medium">{ref.name}</span>
						<span class="text-xs text-base-content/50">({ref.source_id})</span>
					</li>
				{/each}
			</ul>

			<p class="mb-5 text-sm text-base-content/70">
				Target: <span class="font-semibold">{pendingMerge.candidate.name}</span>
				· {pendingMerge.candidate.distance_km.toFixed(2)} km away
				{#each pendingMerge.candidate.sources as ref}
					<span class="ml-1 badge badge-outline badge-sm">{ref.source}</span>
				{/each}
			</p>

			<div class="modal-action">
				<button class="btn btn-ghost btn-sm" onclick={cancelMerge}>Cancel</button>
				<button class="btn btn-sm btn-primary" onclick={confirmMerge}>Confirm merge</button>
			</div>
		{/if}
	</div>
	<form method="dialog" class="modal-backdrop">
		<button onclick={cancelMerge}>close</button>
	</form>
</dialog>

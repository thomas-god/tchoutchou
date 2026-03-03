<script lang="ts">
	import {
		fetchMergeCandidates,
		type MergeCandidateGroup
	} from '$lib/remote/mergeCandidate.remote';

	let maxDistanceKm = $state(1.0);
	let candidatesPromise: Promise<MergeCandidateGroup[]> | null = $state(null);

	function load() {
		candidatesPromise = fetchMergeCandidates({ maxDistanceKm });
	}
</script>

<div class="mx-auto max-w-4xl p-6">
	<h1 class="mb-6 text-2xl font-bold">Station Merge Candidates</h1>

	<form
		class="mb-6 flex items-end gap-4"
		onsubmit={(e) => {
			e.preventDefault();
			load();
		}}
	>
		<label class="flex flex-col gap-1">
			<span class="text-sm font-semibold">Max distance (km)</span>
			<input
				type="number"
				min="0.1"
				max="50"
				step="0.1"
				bind:value={maxDistanceKm}
				class="input-bordered input input-sm w-32"
			/>
		</label>
		<button type="submit" class="btn btn-sm btn-primary">Search</button>
	</form>

	{#if candidatesPromise === null}
		<p class="text-base-content/50">Configure a distance and click Search.</p>
	{:else}
		{#await candidatesPromise}
			<div class="flex justify-center py-12">
				<span class="loading loading-lg loading-spinner text-primary"></span>
			</div>
		{:then groups}
			{#if groups.length === 0}
				<p class="text-base-content/60">
					No stations with merge candidates within {maxDistanceKm} km.
				</p>
			{:else}
				<p class="mb-4 text-sm text-base-content/60">
					{groups.length} station{groups.length !== 1 ? 's' : ''} with nearby candidates within {maxDistanceKm}
					km
				</p>
				<div class="flex flex-col gap-2">
					{#each groups as group (group.id)}
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
										</tr>
									</thead>
									<tbody>
										{#each group.candidates as candidate (candidate.id)}
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
	{/if}
</div>

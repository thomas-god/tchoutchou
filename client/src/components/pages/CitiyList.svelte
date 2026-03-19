<script lang="ts">
	import { type CityWithExtraInformation } from '$lib/remote/cities.remote';
	import { type Label } from '$lib/remote/labels.remote';
	import leaflet, { CircleMarker, LayerGroup } from 'leaflet';

	let {
		cities,
		onRemoveLabel,
		availableLabels = [],
		onAddLabel,
		onSetParent
	}: {
		cities: CityWithExtraInformation[];
		onRemoveLabel: (cityId: number, labelId: number) => Promise<void>;
		availableLabels: Label[];
		onAddLabel: (cityId: number, labelId: number) => Promise<void>;
		onSetParent: (cityId: number, parentId: number | null) => Promise<void>;
	} = $props();

	let query = $state('');
	let citiesError = $state(false);
	let selectedCity: CityWithExtraInformation | null = $state(null);
	let cityToAddLabelsTo: CityWithExtraInformation | null = $state(null);
	let addCityLabelDialog: HTMLDialogElement;

	let cityToSetParentFor: CityWithExtraInformation | null = $state(null);
	let setParentDialog: HTMLDialogElement;
	let parentSearchQuery = $state('');
	let settingParent = $state(false);
	let setParentError = $state('');

	let parentCandidates = $derived.by(() => {
		if (!cityToSetParentFor) return [];
		const q = parentSearchQuery.toLowerCase();
		return cities.filter(
			(c) => c.id !== cityToSetParentFor!.id && (fuzzyMatch(c.name, q) || fuzzyMatch(c.country, q))
		);
	});

	let labelsNotAssigned = $derived.by(() => {
		if (cityToAddLabelsTo === null) {
			return [];
		}
		return availableLabels.filter(
			(label) => !cityToAddLabelsTo!.labels.some((cityLabel) => cityLabel.id === label.id)
		);
	});

	let filtered = $derived(
		cities.filter((c) => fuzzyMatch(c.name, query) || fuzzyMatch(c.country, query))
	);

	let cityById = $derived.by(() => {
		const map: Map<number | null, CityWithExtraInformation> = new Map();
		for (const city of cities) {
			map.set(city.id, city);
		}

		return map;
	});

	function fuzzyMatch(text: string, pattern: string): boolean {
		if (!pattern) return true;
		const t = text.toLowerCase();
		const p = pattern.toLowerCase();
		let ti = 0;
		for (let pi = 0; pi < p.length; pi++) {
			while (ti < t.length && t[ti] !== p[pi]) ti++;
			if (ti === t.length) return false;
			ti++;
		}
		return true;
	}

	let mapElement: HTMLDivElement;
	let tableContainer: HTMLDivElement;
	let map: any = $state(undefined);
	let citiesLayer: LayerGroup<CircleMarker> = leaflet.layerGroup();
	let highlightMarker: any;

	const computeRadius = () => {
		const zoom = map.getZoom();
		return zoom < 8 ? 1 : Math.max(1, Math.min(10, 2 * zoom - 13));
	};

	const updateMarkerSizes = () => {
		const radius = computeRadius();

		citiesLayer.eachLayer((layer) => (layer as any).setRadius(radius));
	};

	// Init map once the DOM element is bound
	$effect(() => {
		if (map === undefined) {
			map = leaflet.map(mapElement).setView([48.85, 2.35], 5);
			leaflet
				.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
					attribution:
						'© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
				})
				.addTo(map);
			citiesLayer.addTo(map);

			map.on('zoomend', updateMarkerSizes);
		}
	});

	// Re-render all city dots whenever the cities list changes

	$effect(() => {
		if (!map) return;
		citiesLayer.clearLayers();
		const radius = computeRadius();
		for (const city of cities) {
			leaflet
				.circleMarker([city.lat, city.lon], {
					radius
				})
				.bindTooltip(city.name, { permanent: false })
				.on('click', () => {
					selectedCity = city;
				})
				.addTo(citiesLayer);
		}
	});

	// Fly to and highlight the selected city
	$effect(() => {
		if (!map || !selectedCity) return;
		if (highlightMarker) {
			highlightMarker.setLatLng([selectedCity.lat, selectedCity.lon]);
		} else {
			highlightMarker = leaflet
				.circleMarker([selectedCity.lat, selectedCity.lon], {
					radius: 8,
					color: '#ef4444',
					fillColor: '#ef4444',
					fillOpacity: 0.9,
					weight: 2
				})
				.addTo(map);
		}
		highlightMarker.bindPopup(selectedCity.name).openPopup();
		map.flyTo([selectedCity.lat, selectedCity.lon], 10, { duration: 0.8 });

		const row = tableContainer?.querySelector(
			`[data-city-id="${selectedCity.id}"]`
		) as HTMLElement | null;
		if (row && tableContainer) {
			const containerRect = tableContainer.getBoundingClientRect();
			const rowRect = row.getBoundingClientRect();
			const absoluteRowTop = rowRect.top - containerRect.top + tableContainer.scrollTop;
			tableContainer.scrollTo({
				top: absoluteRowTop - tableContainer.clientHeight / 2 + row.offsetHeight / 2,
				behavior: 'smooth'
			});
		}
	});
</script>

<div class="flex min-h-0 flex-1 gap-4 p-4">
	<!-- Left: search + table -->
	<div class="flex min-w-0 flex-1 flex-col gap-2 overflow-hidden">
		{#if citiesError}
			<p class="error status">Failed to load cities.</p>
		{:else}
			<div class="flex items-center gap-4">
				<input
					class="input-bordered input w-full max-w-sm"
					type="search"
					placeholder="Search cities…"
					bind:value={query}
				/>
				<p class="italic">
					{#if cities.length === 0}
						<span class="loading loading-dots">Loading…</span>
					{:else}
						{filtered.length} / {cities.length} cities
					{/if}
				</p>
			</div>
			<div class="overflow-auto" bind:this={tableContainer}>
				<table class="table table-zebra">
					<thead>
						<tr>
							<th>ID</th>
							<th>Name</th>
							<th>Country</th>
							<th>Coordinates</th>
							<th>Parent</th>
							<th>Labels</th>
							<th>Wikidata</th>
							<th>Wikipedia</th>
						</tr>
					</thead>
					<tbody>
						{#each filtered as city (city.id)}
							<tr
								class="hover:bg-base-300"
								class:row-highlight={selectedCity?.id === city.id}
								data-city-id={city.id}
							>
								<td>{city.id}</td>
								<td>{city.name}</td>
								<td>{city.country}</td>
								<td>
									<div class="flex items-center gap-1">
										<span class="font-mono text-sm"
											>{city.lat.toFixed(4)}, {city.lon.toFixed(4)}</span
										>
										<button
											class="btn btn-ghost btn-xs"
											title="Show on map"
											onclick={() => (selectedCity = city)}
										>
											📍
										</button>
									</div>
								</td>
								<td>
									<div class="flex items-center gap-1">
										{#if cityById.has(city.parent)}
											{@const parentCity = cityById.get(city.parent) as CityWithExtraInformation}
											<span class="text-sm">{parentCity.name}</span>
										{:else}
											<span class="text-sm text-base-content/40 italic">—</span>
										{/if}
										<button
											class="btn btn-ghost btn-xs"
											title="Set parent"
											onclick={() => {
												cityToSetParentFor = city;
												setParentError = '';
												setParentDialog.showModal();
											}}>✎</button
										>
									</div>
								</td>
								<td class="flex flex-col items-start gap-2">
									<div class="flex flex-col items-start gap-2">
										<button
											class="btn btn-outline btn-xs btn-primary"
											onclick={() => {
												cityToAddLabelsTo = city;
												addCityLabelDialog.show();
											}}>+</button
										>
									</div>

									<div class="flex flex-wrap items-center gap-1">
										{#each city.labels as label (label.id)}
											<span class="badge flex items-center gap-1 badge-outline badge-sm">
												{label.name}
												{#if onRemoveLabel}
													<button
														class="btn h-auto min-h-0 p-0 leading-none btn-ghost btn-xs"
														title="Remove label"
														onclick={() => onRemoveLabel!(city.id, label.id)}>×</button
													>
												{/if}
											</span>
										{/each}
									</div>
								</td>
								<td>
									{#if city.wikidata}
										<a
											class="link"
											href="https://www.wikidata.org/wiki/{city.wikidata}"
											target="_blank"
											rel="noopener noreferrer">{city.wikidata}</a
										>
									{/if}
								</td>
								<td>
									{#if city.wikipedia}
										{@const [lang, ...rest] = city.wikipedia.split(':')}
										<a
											class="link"
											href="https://{lang}.wikipedia.org/wiki/{rest.join(':')}"
											target="_blank"
											rel="noopener noreferrer">{city.wikipedia}</a
										>
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	</div>

	<dialog bind:this={addCityLabelDialog} class="modal">
		<div class="modal-box">
			<form method="dialog">
				<button class="btn absolute top-2 right-2 btn-circle btn-ghost btn-sm">✕</button>
			</form>
			{#if cityToAddLabelsTo !== undefined}
				<h2 class="mb-4 text-xl font-bold">Add label to city</h2>

				<div class="flex flex-row gap-2">
					{#each labelsNotAssigned as label}
						<span class="badge flex items-center gap-1 badge-outline badge-sm"
							>{label.name}

							<button
								class="btn h-auto min-h-0 p-0 leading-none btn-ghost btn-xs"
								title="Add label"
								onclick={() => {
									onAddLabel(cityToAddLabelsTo!.id, label.id);
									labelsNotAssigned = labelsNotAssigned.filter((l) => label.id != l.id);
								}}>+</button
							>
						</span>
					{:else}
						<p class="italic">No labels</p>
					{/each}
				</div>
			{/if}
		</div>
		<form method="dialog" class="modal-backdrop">
			<button>close</button>
		</form>
	</dialog>

	<dialog bind:this={setParentDialog} class="no-transition modal duration-[0]!">
		<div class="no-transition modal-box flex flex-col gap-4 duration-[0]!">
			<button
				onclick={() => setParentDialog.close()}
				class="btn absolute top-2 right-2 btn-circle btn-ghost btn-sm">✕</button
			>
			{#if cityToSetParentFor}
				<h2 class="text-xl font-bold">Set parent for <em>{cityToSetParentFor.name}</em></h2>

				{#if cityById.has(cityToSetParentFor.parent)}
					{@const currentParent = cityById.get(cityToSetParentFor.parent)}
					<div class="flex items-center gap-2">
						<span class="text-sm"
							>Current: <strong>{currentParent?.name ?? cityToSetParentFor.parent}</strong></span
						>
						<button
							class="btn btn-outline btn-xs btn-error"
							disabled={settingParent}
							onclick={async () => {
								settingParent = true;
								setParentError = '';
								try {
									await onSetParent(cityToSetParentFor!.id, null);
									cityToSetParentFor = { ...cityToSetParentFor!, parent: null };
									setParentDialog.close();
								} catch (err) {
									setParentError = err instanceof Error ? err.message : 'Failed to clear parent.';
								} finally {
									settingParent = false;
								}
							}}>Remove parent</button
						>
					</div>
				{/if}

				<input
					class="input-bordered input input-sm w-full"
					type="search"
					placeholder="Search cities…"
					bind:value={parentSearchQuery}
				/>

				{#if setParentError}
					<p class="text-sm text-error">{setParentError}</p>
				{/if}

				<ul class="max-h-64 divide-y divide-base-200 overflow-y-auto">
					{#each parentCandidates.slice(0, 50) as candidate (candidate.id)}
						<li>
							<button
								class="flex w-full items-center justify-between px-2 py-1 text-left hover:bg-base-200"
								class:font-semibold={candidate.id === cityToSetParentFor.parent}
								disabled={settingParent}
								onclick={async () => {
									settingParent = true;
									setParentError = '';
									try {
										await onSetParent(cityToSetParentFor!.id, candidate.id);
										cityToSetParentFor = { ...cityToSetParentFor!, parent: candidate.id };
										setParentDialog.close();
									} catch (err) {
										setParentError = err instanceof Error ? err.message : 'Failed to set parent.';
									} finally {
										settingParent = false;
									}
								}}
							>
								<span>{candidate.name}</span>
								<span class="text-xs text-base-content/50">{candidate.country}</span>
							</button>
						</li>
					{:else}
						<li class="px-2 py-1 italic text-base-content/50">No matching cities</li>
					{/each}
				</ul>
			{/if}
		</div>
		<form method="dialog" class="modal-backdrop">
			<button>close</button>
		</form>
	</dialog>

	<!-- Right: map -->
	<div class="w-1/2 overflow-hidden rounded-lg" bind:this={mapElement}></div>
</div>

<style>
	@keyframes row-flash {
		0%,
		100% {
			box-shadow: inset 0 0 0 9999px transparent;
		}
		30% {
			box-shadow: inset 0 0 0 9999px oklch(80% 0.15 80 / 0.45);
		}
	}

	.row-highlight {
		animation: row-flash 1.4s ease-out;
	}

	.no-transition {
		transition: none !important;
		transition-duration: 0ms !important;
		animation: none !important;
	}
</style>

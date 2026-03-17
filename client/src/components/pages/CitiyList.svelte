<script lang="ts">
	import { type CityWithExtraInformation } from '$lib/remote/cities.remote';
	import leaflet, { CircleMarker, LayerGroup } from 'leaflet';

	let { cities }: { cities: CityWithExtraInformation[] } = $props();

	let query = $state('');
	let citiesError = $state(false);
	let selectedCity: CityWithExtraInformation | undefined = $state(undefined);

	let filtered = $derived(
		cities.filter((c) => fuzzyMatch(c.name, query) || fuzzyMatch(c.country, query))
	);

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
		const city = selectedCity;
		if (!map || !city) return;
		if (highlightMarker) {
			highlightMarker.setLatLng([city.lat, city.lon]);
		} else {
			highlightMarker = leaflet
				.circleMarker([city.lat, city.lon], {
					radius: 8,
					color: '#ef4444',
					fillColor: '#ef4444',
					fillOpacity: 0.9,
					weight: 2
				})
				.addTo(map);
		}
		highlightMarker.bindPopup(city.name).openPopup();
		map.flyTo([city.lat, city.lon], 10, { duration: 0.8 });

		const row = tableContainer?.querySelector(`[data-city-id="${city.id}"]`) as HTMLElement | null;
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
									<div class="flex flex-wrap gap-1">
										{#each city.labels as label (label.id)}
											<span class="badge badge-outline badge-sm">{label.name}</span>
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
</style>

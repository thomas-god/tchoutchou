<script lang="ts">
	import { fetchCities } from '$lib/remote/cities.remote';
	import leaflet from 'leaflet';

	let query = $state('');
	let selectedCity: { lat: number; lon: number; name: string } | undefined = $state(undefined);

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
	let map: any = $state(undefined);
	let marker: any;

	$effect(() => {
		if (map === undefined) {
			map = leaflet.map(mapElement).setView([48.85, 2.35], 4);
			leaflet
				.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
					attribution:
						'© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
				})
				.addTo(map);
		}
	});

	$effect(() => {
		const city = selectedCity;
		if (!map || !city) return;
		if (marker) {
			marker.setLatLng([city.lat, city.lon]);
		} else {
			marker = leaflet.marker([city.lat, city.lon]).addTo(map);
		}
		marker.bindPopup(city.name).openPopup();
		map.flyTo([city.lat, city.lon], 10, { duration: 0.8 });
	});
</script>

<div class="flex h-screen flex-col overflow-hidden">
	<h1 class="px-4 pt-4 text-2xl">Cities</h1>
	<div class="flex min-h-0 flex-1 gap-4 p-4">
		<!-- Left: search + table -->
		<div class="flex min-w-0 flex-1 flex-col gap-2 overflow-hidden">
			{#await fetchCities(undefined)}
				<p class="loading loading-dots">Loading…</p>
			{:then cities}
				{@const filtered = cities.filter(
					(c) => fuzzyMatch(c.name, query) || fuzzyMatch(c.country, query)
				)}
				<div class="flex items-center gap-4">
					<input
						class="input-bordered input w-full max-w-sm"
						type="search"
						placeholder="Search cities…"
						bind:value={query}
					/>
					<p class="italic">{filtered.length} / {cities.length} cities</p>
				</div>
				<div class="overflow-auto">
					<table class="table table-zebra">
						<thead>
							<tr>
								<th>ID</th>
								<th>Name</th>
								<th>Country</th>
								<th>Coordinates</th>
								<th>Wikidata</th>
								<th>Wikipedia</th>
							</tr>
						</thead>
						<tbody>
							{#each filtered as city (city.id)}
								<tr class="hover:bg-base-300">
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
												onclick={() =>
													(selectedCity = { lat: city.lat, lon: city.lon, name: city.name })}
											>
												📍
											</button>
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
			{:catch}
				<p class="error status">Failed to load cities.</p>
			{/await}
		</div>

		<!-- Right: map -->
		<div class="w-1/2 overflow-hidden rounded-lg" bind:this={mapElement}></div>
	</div>
</div>

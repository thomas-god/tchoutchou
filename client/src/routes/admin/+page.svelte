<script lang="ts">
	import { fetchCities } from '$lib/remote/cities.remote';
</script>

<h1 class="text-2xl">Cities</h1>

{#await fetchCities(undefined)}
	<p class="loading loading-dots">Loading…</p>
{:then cities}
	<p class="italic">{cities.length} cities</p>
	<div class="overflow-x-auto">
		<table class="table table-zebra">
			<thead>
				<tr>
					<th>ID</th>
					<th>Name</th>
					<th>Country</th>
					<th>Lat</th>
					<th>Lon</th>
					<th>Wikidata</th>
					<th>Wikipedia</th>
				</tr>
			</thead>
			<tbody>
				{#each cities as city (city.id)}
					<tr class="hover:bg-base-300">
						<td>{city.id}</td>
						<td>{city.name}</td>
						<td>{city.country}</td>
						<td>{city.lat.toFixed(4)}</td>
						<td>{city.lon.toFixed(4)}</td>
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

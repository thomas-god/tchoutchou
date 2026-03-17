<script lang="ts">
	import { fetchCities } from '$lib/remote/cities.remote';
	import CitiyList from '../../components/pages/CitiyList.svelte';
</script>

<div class="flex h-screen flex-col overflow-hidden">
	<h1 class="px-4 pt-4 text-2xl">Cities</h1>

	{#await fetchCities(undefined)}
		<p class="loading loading-dots">Loading…</p>
	{:then cities}
		<CitiyList {cities} />
	{:catch}
		<p class="error status">Failed to load cities.</p>
	{/await}
</div>

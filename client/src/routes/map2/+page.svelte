<script lang="ts">
	import { fetchAllStations } from '$lib/remote/mergeCandidate.remote';
	import type { Node } from '$lib/api/schedule';
	import StationsMap from '../../components/molecules/StationsMap.svelte';

	const stationsPromise = fetchAllStations().then((stations) =>
		stations.map((s): Node => ({ ...s, id: s.id.toString() }))
	);
</script>

{#await stationsPromise}
	<div class="flex w-full flex-col items-center">
		<span class="loading mx-auto mt-12 loading-xl loading-spinner text-center"></span>
	</div>
{:then nodes}
	<div class="h-lvh w-full">
		<StationsMap stations={nodes} />
	</div>
{/await}

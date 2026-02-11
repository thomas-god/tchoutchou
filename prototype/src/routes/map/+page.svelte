<script lang="ts">
	import { fetchNodesQuery } from '$lib/remote/graph.remote';
	import dayjs from 'dayjs';
	import StationsMap from '../../components/molecules/StationsMap.svelte';

	let nodesPromise = fetchNodesQuery({ from: dayjs().toISOString() });
</script>

{#await nodesPromise}
	<div class="flex w-full flex-col items-center">
		<span class="loading mx-auto mt-12 loading-xl loading-spinner text-center"></span>
	</div>
{:then nodes}
	<div class="h-lvh w-full">
		<StationsMap stations={nodes} />
	</div>
{/await}

<script lang="ts">
	import { fetchNodesQuery } from '$lib/remote/graph.remote';
	import dayjs from 'dayjs';
	import StationsMap from '../../components/molecules/StationsMap.svelte';

	let nodes = await fetchNodesQuery({ from: dayjs().toISOString() });

	let bounds = $derived({
		lat: {
			min: Math.min(...nodes.map((node) => node.lat)),
			max: Math.max(...nodes.map((node) => node.lat))
		},
		lon: {
			min: Math.min(...nodes.map((node) => node.lon)),
			max: Math.max(...nodes.map((node) => node.lon))
		}
	});
</script>

<div class="h-lvh w-full">
	<StationsMap stations={nodes} {bounds} />
</div>

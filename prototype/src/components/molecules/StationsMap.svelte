<script lang="ts">
	import { onDestroy } from 'svelte';
	import leaflet from 'leaflet';
	import type { Node } from '$lib/schedule';

	interface Props {
		stations: Node[];
	}

	let { stations }: Props = $props();

	let mapElement: HTMLDivElement;
	let map: any = $state(undefined);

	let bounds = $derived({
		lat: {
			min: Math.min(...stations.map((node) => node.lat)),
			max: Math.max(...stations.map((node) => node.lat))
		},
		lon: {
			min: Math.min(...stations.map((node) => node.lon)),
			max: Math.max(...stations.map((node) => node.lon))
		}
	});

	const icon = leaflet.divIcon({
		html: `
			  <img src="/icons/station.svg" alt="Train station" class="w-3.5 h-3.5"/>
			`
	});
	let markersLayer = new leaflet.LayerGroup();
	let markers = $derived(
		stations.map((station) => ({
			id: station.id,
			marker: leaflet.marker([station.lat, station.lon], { icon }).bindPopup(`${station.name}`)
		}))
	);

	$effect(() => {
		if (map === undefined) {
			console.log(mapElement);
			map = leaflet.map(mapElement);
			leaflet
				.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
					attribution:
						'© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
				})
				.addTo(map);
			map.addLayer(markersLayer);
		}

		markersLayer.clearLayers();

		for (const { marker } of markers) {
			markersLayer.addLayer(marker);
		}

		if (stations.length > 0) {
			map.fitBounds([
				[bounds.lat.min, bounds.lon.min],
				[bounds.lat.max, bounds.lon.max]
			]);
		}
	});

	onDestroy(async () => {
		if (map) {
			map.remove();
		}
	});
</script>

<div class="h-full w-full" bind:this={mapElement}></div>

<script lang="ts">
	import { displayDuration } from '$lib';
	import type { Destination } from '$lib/api/stop_schedules';
	import { onMount, onDestroy } from 'svelte';

	interface Props {
		origin: {
			lat: number;
			lon: number;
		};
		destinations: Destination[];
	}

	let { origin, destinations: destinations }: Props = $props();

	let mapElement: HTMLDivElement;
	let map: any;
	let leaflet: any;

	let minLat = $derived(Math.min(...destinations.map((destination) => destination.stop.lat)));
	let maxLat = $derived(Math.max(...destinations.map((destination) => destination.stop.lat)));
	let minLon = $derived(Math.min(...destinations.map((destination) => destination.stop.lon)));
	let maxLon = $derived(Math.max(...destinations.map((destination) => destination.stop.lon)));
	let deltaLat = $derived((maxLat - minLat) * 0.001);
	let deltaLon = $derived((maxLon - minLon) * 0.001);

	onMount(async () => {
		leaflet = await import('leaflet');

		map = leaflet.map(mapElement);

		if (destinations.length > 0) {
			map.fitBounds([
				[minLat - deltaLat, minLon - deltaLon],
				[maxLat + deltaLat, maxLon + deltaLon]
			]);
		} else {
			map.setView([origin.lat, origin.lon], 13);
		}

		const icon = leaflet.icon({
			iconUrl: 'src/static/station.svg',
			iconSize: [30, 30]
		});

		for (const destination of destinations) {
			leaflet
				.marker([destination.stop.lat, destination.stop.lon], { icon })
				.addTo(map)
				.bindPopup(`${destination.stop.name} (${displayDuration(destination.duration)})`);
		}

		leaflet
			.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
				attribution:
					'© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
			})
			.addTo(map);
	});

	onDestroy(async () => {
		if (map) {
			console.log('Unloading Leaflet map.');
			map.remove();
		}
	});
</script>

<div class="h-svh w-full" bind:this={mapElement}></div>

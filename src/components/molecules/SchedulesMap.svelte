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

		for (const destination of destinations) {
			const icon = leaflet.divIcon({
				html: `
        <img src="/icons/station.svg" alt="Train station" class="w-8 h-8"/>
      `
			});
			leaflet
				.marker([destination.stop.lat, destination.stop.lon], { icon })
				.addTo(map)
				.bindPopup(`${destination.stop.name} (${displayDuration(destination.duration)})`);
		}

		const startIcon = leaflet.divIcon({
			html: `
        <img src="/icons/city.svg" alt="City" class="w-12 h-12 z-10"/>
      `
		});
		leaflet.marker([origin.lat, origin.lon], { icon: startIcon, zIndexOffset: 1000 }).addTo(map);

		leaflet
			.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
				attribution:
					'© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
			})
			.addTo(map);
	});

	onDestroy(async () => {
		if (map) {
			map.remove();
		}
	});
</script>

<div class="h-full w-full" bind:this={mapElement}></div>

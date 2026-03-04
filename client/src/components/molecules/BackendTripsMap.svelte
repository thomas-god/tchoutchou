<script lang="ts">
	import { displayDuration } from '$lib';
	import { onDestroy } from 'svelte';
	import leaflet from 'leaflet';
	import type {
		BackendDestinationResult,
		BackendStation
	} from '$lib/remote/backend-schedules.remote';

	interface Props {
		origin: BackendStation;
		destinations: BackendDestinationResult[];
		bounds: { lat: { min: number; max: number }; lon: { min: number; max: number } };
		selectedDestination: undefined | BackendDestinationResult;
	}

	let { origin, destinations, selectedDestination, bounds }: Props = $props();

	let mapElement: HTMLDivElement;
	let map: any = $state(undefined);

	const icon = leaflet.divIcon({
		html: `
			  <img src="/icons/station.svg" alt="Train station" class="w-3.5 h-3.5"/>
			`
	});
	let markersLayer = new leaflet.LayerGroup();
	let markers = $derived(
		destinations.map((destination) => ({
			id: destination.station.id,
			marker: leaflet
				.marker([destination.station.lat, destination.station.lon], { icon })
				.bindPopup(
					`${destination.station.name} (${displayDuration(destination.duration)}, ${destination.connections} correspondance(s))`
				)
		}))
	);

	$effect(() => {
		if (selectedDestination !== undefined) {
			const marker = markers.find((marker) => marker.id === selectedDestination!.station.id);
			if (marker !== undefined) {
				marker.marker.openPopup();
			}
		}
	});

	$effect(() => {
		if (map === undefined) {
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

		if (destinations.length > 0) {
			map.fitBounds([
				[bounds.lat.min, bounds.lon.min],
				[bounds.lat.max, bounds.lon.max]
			]);
		} else {
			map.setView([origin.lat, origin.lon], 13);
		}

		const startIcon = leaflet.divIcon({
			html: `
		    <img src="/icons/city.svg" alt="City" class="w-6 h-6 z-10"/>
		  `
		});
		markersLayer.addLayer(
			leaflet.marker([origin.lat, origin.lon], { icon: startIcon, zIndexOffset: 1000 })
		);
	});

	onDestroy(async () => {
		if (map) {
			map.remove();
		}
	});
</script>

<div class="h-full w-full" bind:this={mapElement}></div>

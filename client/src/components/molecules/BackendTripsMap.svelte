<script lang="ts">
	import { displayDuration } from '$lib';
	import { onDestroy } from 'svelte';
	import leaflet from 'leaflet';
	import 'leaflet.markercluster';
	import 'leaflet.markercluster/dist/MarkerCluster.css';
	import 'leaflet.markercluster/dist/MarkerCluster.Default.css';
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

	let markersLayer = (leaflet as any).markerClusterGroup({ chunkedLoading: true });
	let highlightLayer = leaflet.layerGroup();
	let previouslyHighlighted: any = undefined;

	let markers = $derived(
		destinations.map((destination) => ({
			id: destination.station.id,
			marker: leaflet
				.circleMarker([destination.station.lat, destination.station.lon])
				.bindPopup(
					`${destination.station.name} (${displayDuration(destination.duration)}, ${destination.connections} correspondance(s))` +
						(destination.visitedStations.length > 0
							? `<br>Via\u00a0: ${destination.visitedStations.map((s) => s.name).join(' \u2192 ')}`
							: '')
				)
		}))
	);

	$effect(() => {
		if (map === undefined) return;

		// Restore previously highlighted marker back to the cluster
		if (previouslyHighlighted !== undefined) {
			highlightLayer.removeLayer(previouslyHighlighted);
			markersLayer.addLayer(previouslyHighlighted);
			previouslyHighlighted = undefined;
		}

		if (selectedDestination !== undefined) {
			const entry = markers.find((m) => m.id === selectedDestination!.station.id);
			if (entry !== undefined) {
				// Move the marker out of the cluster so it renders individually
				markersLayer.removeLayer(entry.marker);
				highlightLayer.addLayer(entry.marker);
				previouslyHighlighted = entry.marker;

				map.flyTo([selectedDestination.station.lat, selectedDestination.station.lon], 8, {
					duration: 0.6,
					animate: true
				});
				entry.marker.openPopup();
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
			map.addLayer(highlightLayer);
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

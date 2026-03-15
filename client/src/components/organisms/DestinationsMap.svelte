<script lang="ts">
	import { displayDuration } from '$lib';
	import { onDestroy } from 'svelte';
	import leaflet from 'leaflet';
	import 'leaflet.markercluster';
	import 'leaflet.markercluster/dist/MarkerCluster.css';
	import 'leaflet.markercluster/dist/MarkerCluster.Default.css';
	import type { DestinationResult, City } from '$lib/remote/destinations.remote';

	interface Props {
		origin: City | undefined;
		destinations: DestinationResult[];
		bounds: { lat: { min: number; max: number }; lon: { min: number; max: number } } | undefined;
		selectedDestination: undefined | DestinationResult;
		onDestinationSelect?: (destination: DestinationResult) => void;
	}

	let { origin, destinations, selectedDestination, bounds, onDestinationSelect }: Props = $props();

	let mapElement: HTMLDivElement;
	let map: any = $state(undefined);

	let markersLayer = (leaflet as any).markerClusterGroup({ chunkedLoading: true });
	let highlightLayer = leaflet.layerGroup();
	let routeLayer = leaflet.layerGroup();
	let previouslyHighlighted: any = undefined;

	let markers = $derived(
		destinations.map((destination) => {
			const marker = leaflet
				.circleMarker([destination.station.lat, destination.station.lon])
				.on('click', () => {
					if (onDestinationSelect) {
						onDestinationSelect(destination);
					}
				});

			return {
				id: destination.station.id,
				marker
			};
		})
	);

	$effect(() => {
		if (map === undefined) return;

		// Restore previously highlighted marker back to the cluster
		if (previouslyHighlighted !== undefined) {
			highlightLayer.removeLayer(previouslyHighlighted);
			markersLayer.addLayer(previouslyHighlighted);
			previouslyHighlighted = undefined;
		}

		// Clear previous route lines
		routeLayer.clearLayers();

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

				// Draw route lines from origin through intermediate stations to destination
				if (origin !== undefined) {
					const routePoints: [number, number][] = [
						[origin.lat, origin.lon],
						...selectedDestination.visitedStations.map((s) => [s.lat, s.lon] as [number, number]),
						[selectedDestination.station.lat, selectedDestination.station.lon]
					];

					// Draw the main route line
					const routeLine = leaflet
						.polyline(routePoints, {
							color: '#3b82f6',
							weight: 3,
							opacity: 0.7,
							smoothFactor: 1
						})
						.addTo(routeLayer);

					// Add markers for intermediate stations
					selectedDestination.visitedStations.forEach((station) => {
						const intermediateMarker = leaflet
							.circleMarker([station.lat, station.lon], {
								radius: 5,
								fillColor: '#f59e0b',
								color: '#fff',
								weight: 2,
								opacity: 1,
								fillOpacity: 0.8
							})
							.bindPopup(`${station.name}`)
							.addTo(routeLayer);
					});
				}
			}
		}
	});

	$effect(() => {
		if (map === undefined) {
			map = leaflet.map(mapElement, { zoomControl: false });
			leaflet
				.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
					attribution:
						'© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
				})
				.addTo(map);
			map.addLayer(routeLayer);
			map.addLayer(markersLayer);
			map.addLayer(highlightLayer);
		}

		markersLayer.clearLayers();

		for (const { marker } of markers) {
			markersLayer.addLayer(marker);
		}

		if (destinations.length > 0 && bounds !== undefined) {
			map.fitBounds([
				[bounds.lat.min, bounds.lon.min],
				[bounds.lat.max, bounds.lon.max]
			]);
		} else if (origin !== undefined) {
			map.setView([origin.lat, origin.lon], 13);
		} else {
			map.setView([46.5, 2.3], 6);
		}

		const startIcon = leaflet.divIcon({
			html: `
		    <img src="/icons/city.svg" alt="City" class="w-6 h-6 z-10"/>
		  `
		});
		if (origin !== undefined) {
			markersLayer.addLayer(
				leaflet.marker([origin.lat, origin.lon], { icon: startIcon, zIndexOffset: 1000 })
			);
		}
	});

	onDestroy(async () => {
		if (map) {
			map.remove();
		}
	});
</script>

<div class="h-full w-full" bind:this={mapElement}></div>

<script lang="ts">
	import { onDestroy } from 'svelte';
	import leaflet from 'leaflet';
	import '@geoman-io/leaflet-geoman-free';
	import '@geoman-io/leaflet-geoman-free/dist/leaflet-geoman.css';
	import type { Node } from '$lib/api/schedule';

	// Core zone data type (serializable)
	interface Zone {
		id: string;
		name: string;
		category: 'sea' | 'mountain';
		coordinates: { lat: number; lng: number }[];
	}

	interface Props {
		stations: Node[];
	}

	let { stations }: Props = $props();

	let mapElement: HTMLDivElement;
	let map: any = $state(undefined);

	// Form state for new polygon
	let newZoneName = $state('');
	let newZoneCategory = $state<'sea' | 'mountain'>('sea');
	let isDrawing = $state(false);

	// Core zone data (serializable, can be persisted)
	let zones: Zone[] = $state([]);

	// Map of zone ID to Leaflet layer (for rendering only)
	let zoneLayers: Map<string, any> = new Map();

	// Editing state
	let editingZoneId: string | null = $state(null);
	let editingName = $state('');
	let editingCategory = $state<'sea' | 'mountain'>('sea');
	let originalCoordinates: any = $state(null);

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
			map = leaflet.map(mapElement);
			leaflet
				.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
					attribution:
						'© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
				})
				.addTo(map);

			// Draw markers for each station
			map.addLayer(markersLayer);
			markersLayer.clearLayers();
			for (const { marker } of markers) {
				markersLayer.addLayer(marker);
			}

			// Add edit controls only (no drawing controls by default)
			map.pm.addControls({
				position: 'topleft',
				drawPolygon: false,
				drawCircleMarker: false,
				drawMarker: false,
				drawPolyline: false,
				drawRectangle: false,
				drawCircle: false,
				drawText: false,
				editControls: false,
				rotateMode: false
			});

			map.on('pm:create', (e: any) => {
				const layer = e.layer;
				const coordinates = layer.getLatLngs()[0];

				// Create new zone with core data only
				const zoneId = crypto.randomUUID();
				const zone: Zone = {
					id: zoneId,
					name: newZoneName || 'Unnamed Zone',
					category: newZoneCategory,
					coordinates
				};

				// Store zone data and layer separately
				zones = [...zones, zone];
				zoneLayers.set(zoneId, layer);

				// Reset form and stop drawing mode
				newZoneName = '';
				newZoneCategory = 'sea';
				isDrawing = false;
				map.pm.disableDraw();
			});
		}

		if (stations.length > 0) {
			map.fitBounds([
				[bounds.lat.min, bounds.lon.min],
				[bounds.lat.max, bounds.lon.max]
			]);
		}
	});

	const startDrawing = () => {
		if (map && !isDrawing) {
			isDrawing = true;
			map.pm.enableDraw('Polygon', {
				snappable: false
			});
		}
	};

	const cancelDrawing = () => {
		if (map && isDrawing) {
			isDrawing = false;
			map.pm.disableDraw();
		}
	};

	const removeZone = (id: string) => {
		const layer = zoneLayers.get(id);
		if (layer) {
			map.removeLayer(layer);
			zoneLayers.delete(id);
		}
		zones = zones.filter((z) => z.id !== id);
		if (editingZoneId === id) {
			editingZoneId = null;
			editingName = '';
			editingCategory = 'sea';
		}
	};

	const startEditingZone = (id: string) => {
		const zone = zones.find((z) => z.id === id);
		const layer = zoneLayers.get(id);
		if (zone && layer) {
			// Stop any current editing
			if (editingZoneId) {
				stopEditingZone();
			}

			editingZoneId = id;
			editingName = zone.name;
			editingCategory = zone.category;
			// Store original coordinates for potential rollback
			originalCoordinates = JSON.parse(JSON.stringify(zone.coordinates));

			layer.pm.enable();
		}
	};

	const stopEditingZone = () => {
		if (editingZoneId) {
			const layer = zoneLayers.get(editingZoneId);
			if (layer) {
				// Revert to original coordinates
				if (originalCoordinates) {
					layer.setLatLngs(originalCoordinates);
				}
				layer.pm.disable();
			}
			editingZoneId = null;
			editingName = '';
			editingCategory = 'sea';
			originalCoordinates = null;
		}
	};

	const saveZoneEdit = () => {
		if (editingZoneId) {
			const layer = zoneLayers.get(editingZoneId);
			// Get current coordinates directly from the layer
			const currentCoordinates = layer ? layer.getLatLngs()[0] : null;

			// Save all changes including coordinates
			zones = zones.map((z) =>
				z.id === editingZoneId
					? { ...z, name: editingName, category: editingCategory, coordinates: currentCoordinates }
					: z
			);

			if (layer) {
				layer.pm.disable();
			}
			editingZoneId = null;
			editingName = '';
			editingCategory = 'sea';
			originalCoordinates = null;
		}
	};

	onDestroy(async () => {
		if (map) {
			map.remove();
		}
	});
</script>

<div class="flex h-full">
	<!-- Sidebar for polygon management -->
	<div class="w-64 overflow-y-auto bg-base-200 p-4">
		<h3 class="mb-4 text-lg font-semibold">Zone Management</h3>

		<!-- Form to create new polygon -->
		<div class="mb-6">
			<label class="label">
				<span class="label-text">Category</span>
			</label>
			<select
				bind:value={newZoneCategory}
				class="select-bordered select w-full select-sm"
				disabled={isDrawing}
			>
				<option value="sea">🌊 Sea</option>
				<option value="mountain">⛰️ Mountain</option>
			</select>

			<label class="label mt-2">
				<span class="label-text">Zone Name</span>
			</label>
			<input
				type="text"
				bind:value={newZoneName}
				placeholder="Enter zone name"
				class="input-bordered input input-sm w-full"
				disabled={isDrawing}
			/>

			<div class="mt-2 flex gap-2">
				{#if !isDrawing}
					<button
						onclick={startDrawing}
						class="btn flex-1 btn-sm btn-primary"
						disabled={!newZoneName.trim()}
					>
						Draw Polygon
					</button>
				{:else}
					<button onclick={cancelDrawing} class="btn flex-1 btn-sm btn-error"> Cancel </button>
					<div class="mt-2 text-sm text-info">Click on map to draw polygon</div>
				{/if}
			</div>
		</div>

		<!-- List of saved zones -->
		<div>
			<h4 class="mb-2 font-semibold">Saved Zones ({zones.length})</h4>
			<div class="space-y-2">
				{#each zones as zone (zone.id)}
					<div class="card bg-base-100 p-2 shadow-sm">
						{#if editingZoneId === zone.id}
							<!-- Editing mode -->
							<div class="space-y-2">
								<select
									bind:value={editingCategory}
									class="select-bordered select w-full select-xs"
								>
									<option value="sea">🌊 Sea</option>
									<option value="mountain">⛰️ Mountain</option>
								</select>
								<input
									type="text"
									bind:value={editingName}
									class="input-bordered input input-xs w-full"
									placeholder="Zone name"
								/>
								<div class="flex gap-1">
									<button onclick={saveZoneEdit} class="btn flex-1 btn-xs btn-success">
										Save
									</button>
									<button onclick={stopEditingZone} class="btn flex-1 btn-ghost btn-xs">
										Cancel
									</button>
								</div>
								<div class="text-xs text-info">Click on map to edit shape</div>
							</div>
						{:else}
							<!-- View mode -->
							<div class="flex items-center justify-between">
								<div class="flex flex-col">
									<span class="text-sm font-medium">{zone.name}</span>
									<span class="text-xs text-base-content/60">
										{zone.category === 'sea' ? '🌊 Sea' : '⛰️ Mountain'}
									</span>
								</div>
								<div class="flex gap-1">
									<button
										onclick={() => startEditingZone(zone.id)}
										class="btn btn-ghost btn-xs"
										title="Edit zone"
									>
										✏️
									</button>
									<button
										onclick={() => removeZone(zone.id)}
										class="btn btn-ghost btn-xs"
										title="Delete zone"
									>
										🗑️
									</button>
								</div>
							</div>
						{/if}
					</div>
				{/each}
			</div>
		</div>
	</div>

	<!-- Map -->
	<div class="flex-1" bind:this={mapElement}></div>
</div>

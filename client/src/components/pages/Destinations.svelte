<script lang="ts">
	import { displayDuration } from '$lib';
	import { fade } from 'svelte/transition';
	import { autocompleteStation, fetchDestinations } from '$lib/remote/destinations.remote';
	import type { DestinationResult, City } from '$lib/remote/destinations.remote';
	import DestinationsMap from '../organisms/DestinationsMap.svelte';

	let stop: { id: number; name: string } | undefined = $state(undefined);

	// Station autocomplete state
	let query = $state('');
	let autocompleteOptions: { id: number; name: string }[] = $state([]);
	let autocompleteTimer: any;

	let result: { origin: City | null; destinations: DestinationResult[] } | undefined =
		$state(undefined);
	let loading = $state(false);
	let selectedDestination: DestinationResult | undefined = $state(undefined);
	let showResults = $state(false);

	const debounce = () => {
		stop = undefined;
		clearTimeout(autocompleteTimer);
		autocompleteTimer = setTimeout(async () => {
			if (query.length >= 2) {
				autocompleteOptions = await autocompleteStation(query);
			} else {
				autocompleteOptions = [];
			}
		}, 200);
	};

	$effect(() => {
		const currentStop = stop;

		if (currentStop === undefined) {
			result = undefined;
			showResults = false;
			return;
		}

		loading = true;
		showResults = false;
		selectedDestination = undefined;
		let cancelled = false;

		fetchDestinations({ from: currentStop.id, maxConnections: 2 }).then((r) => {
			if (!cancelled) {
				result = r;
				loading = false;
			}
		});

		return () => {
			cancelled = true;
		};
	});

	let bounds = $derived.by(() => {
		const destinations = result?.destinations ?? [];
		if (destinations.length === 0) return undefined;
		return {
			lat: {
				min: Math.min(...destinations.map((d) => d.station.lat)),
				max: Math.max(...destinations.map((d) => d.station.lat))
			},
			lon: {
				min: Math.min(...destinations.map((d) => d.station.lon)),
				max: Math.max(...destinations.map((d) => d.station.lon))
			}
		};
	});

	let sortedDestinations = $derived.by(() =>
		[...(result?.destinations ?? [])].sort((a, b) => a.duration - b.duration)
	);
</script>

<div class="relative h-lvh w-full">
	<DestinationsMap
		origin={result?.origin ?? undefined}
		destinations={result?.destinations ?? []}
		{selectedDestination}
		{bounds}
		onDestinationSelect={(destination) => (selectedDestination = destination)}
	/>

	<!-- Sidebar: form always visible; results visible only on desktop (sm+) -->
	<div
		class="absolute top-4 right-4 left-4 z-1000 flex flex-col gap-3 sm:right-auto sm:max-h-[calc(100lvh-2rem)] sm:w-80"
	>
		<!-- Form controls panel -->
		<div class="flex flex-col pt-1">
			<label class="input input-md w-full rounded-lg">
				<img src="/icons/locomotive.svg" alt="Steam train locomotive" class="h-4 w-4" />
				<input
					type="search"
					bind:value={query}
					oninput={debounce}
					placeholder="Je souhaite partir de ..."
					class="grow"
				/>
			</label>
			{#if stop === undefined && autocompleteOptions.length > 0}
				<ul class="flex flex-col items-start rounded-b-lg bg-base-100 p-2">
					{#each autocompleteOptions as option (option.id)}
						<li class="w-full p-0.5 hover:bg-base-300">
							<button
								class="w-full text-start"
								onclick={() => {
									stop = option;
									query = option.name;
									autocompleteOptions = [];
								}}
							>
								{option.name}
							</button>
						</li>
					{/each}
				</ul>
			{/if}
		</div>

		<!-- Results panel (desktop only) -->
		{#if loading}
			<div class="hidden rounded-lg bg-base-300 p-3 shadow-lg sm:block">
				<span class="loading loading-xl self-center loading-dots"></span>
			</div>
		{:else if result !== undefined}
			{#if result.origin !== null}
				<div
					class="hidden min-h-0 flex-col gap-1 overflow-hidden rounded-lg bg-base-300 p-3 shadow-lg sm:flex"
				>
					<h2 class="shrink-0 text-sm font-semibold">
						{result.destinations.length} destinations trouvées
					</h2>
					<div class="overflow-y-auto">
						{@render destinationItems()}
					</div>
				</div>
			{:else}
				<div class="hidden rounded-lg bg-base-300 p-3 shadow-lg sm:block">
					<p class="text-warning">Gare de départ introuvable dans la base de données.</p>
				</div>
			{/if}
		{/if}
	</div>

	<!-- Mobile: floating toggle button -->
	{#if !showResults && (loading || (result !== undefined && result.origin !== null))}
		<div class="absolute bottom-14 left-1/2 z-1000 -translate-x-1/2 sm:hidden">
			<button class="btn shadow-lg btn-primary" onclick={() => (showResults = true)}>
				{#if loading}
					<span class="loading loading-sm loading-spinner"></span>
					Recherche en cours…
				{:else}
					{result!.destinations.length} destinations ➡️
				{/if}
			</button>
		</div>
	{/if}

	<!-- Mobile: bottom sheet results -->
	{#if showResults && result?.origin !== null}
		<div
			class="absolute inset-x-0 bottom-0 z-1000 flex max-h-[60lvh] flex-col rounded-t-xl bg-base-300 shadow-lg sm:hidden"
		>
			<div class="flex shrink-0 items-center justify-between border-b border-base-200 p-3">
				<h2 class="text-sm font-semibold">
					{result!.destinations.length} destinations trouvées
				</h2>
				<button class="btn btn-circle btn-ghost btn-sm" onclick={() => (showResults = false)}
					>✕</button
				>
			</div>
			<div class="overflow-y-auto p-3">
				{@render destinationItems()}
			</div>
		</div>
	{/if}
</div>

{#snippet destinationItems()}
	{#each sortedDestinations as destination (destination.station.id)}
		<div class="p-1 hover:bg-base-100" in:fade|global out:fade|global={{ duration: 50 }}>
			<button onclick={() => (selectedDestination = destination)} class="w-full text-start">
				<h3 class="text-md font-semibold">{destination.station.name}</h3>
				<p class="text-xs italic">
					{displayDuration(destination.duration)}
					<span>
						·
						{#if destination.connections > 0}
							{destination.connections} correspondance(s)
						{:else}
							direct
						{/if}
					</span>
				</p>
			</button>
		</div>
	{:else}
		<p class="text-warning">Pas de destination trouvée pour cette gare</p>
	{/each}
{/snippet}

<script lang="ts">
	import { displayDuration, filterAndSortDestinations, type DestinationFilters } from '$lib';
	import { fade } from 'svelte/transition';
	import { autocompleteStation, fetchDestinations } from '$lib/remote/destinations.remote';
	import type { DestinationResult, City } from '$lib/remote/destinations.remote';
	import DestinationsMap from '../organisms/DestinationsMap.svelte';
	import DestinationsFilters from '../organisms/DestinationsFilters.svelte';
	import DestinationCard from '../molecules/DestinationCard.svelte';

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
	let filtersDialog: HTMLDialogElement;

	// Filter state
	let filters: DestinationFilters = $state({
		duration: { min: 0, max: 24 * 3600 },
		maxConnections: 2
	});

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

		fetchDestinations({ from: currentStop.id }).then((r) => {
			if (!cancelled) {
				result = r;
				sortedDestinations = filterAndSortDestinations(result?.destinations ?? [], filters);
				loading = false;
			}
		});

		return () => {
			cancelled = true;
		};
	});

	let bounds = $derived.by(() => {
		const destinations = sortedDestinations;
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

	let sortedDestinations: DestinationResult[] = $state([]);
	let debounceTimer: ReturnType<typeof setTimeout>;
	const debouncedDestinationsFilters = () => {
		clearTimeout(debounceTimer);
		debounceTimer = setTimeout(() => {
			sortedDestinations = filterAndSortDestinations(result?.destinations ?? [], filters);
		}, 300);
	};
</script>

<div class="relative h-lvh w-full">
	<DestinationsMap
		origin={result?.origin ?? undefined}
		destinations={sortedDestinations}
		{selectedDestination}
		{bounds}
		onDestinationSelect={(destination) => (selectedDestination = destination)}
	/>

	<div
		class="absolute top-4 right-4 left-4 z-1000 flex max-h-[calc(100lvh-2rem)] flex-col gap-3 overflow-y-auto sm:right-auto sm:w-80"
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
				{#if loading}
					<span class="loading loading-xl self-center loading-dots"></span>
				{/if}
				<button class="btn btn-circle btn-ghost" onclick={() => filtersDialog.showModal()}>
					<img src="/icons/filter.svg" alt="Filter icon" class="h-4 w-4" />
				</button>
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

		{#if selectedDestination}
			<div class="hidden sm:block" transition:fade={{ duration: 150 }}>
				<DestinationCard
					destination={selectedDestination}
					originName={result?.origin?.name}
					onClose={() => (selectedDestination = undefined)}
				/>
			</div>
		{/if}
	</div>

	{#if selectedDestination}
		<div
			class="absolute right-0 bottom-0 left-0 z-1000 sm:hidden"
			transition:fade={{ duration: 150 }}
		>
			<DestinationCard
				destination={selectedDestination}
				originName={result?.origin?.name}
				onClose={() => (selectedDestination = undefined)}
			/>
		</div>
	{/if}

	<!-- Filters Modal -->
	<dialog bind:this={filtersDialog} class="modal">
		<div class="modal-box">
			<form method="dialog">
				<button class="btn absolute top-2 right-2 btn-circle btn-ghost btn-sm">✕</button>
			</form>
			<h2 class="mb-4 text-xl font-bold">Filtres</h2>
			<DestinationsFilters
				bind:filters={
					() => filters,
					(v) => {
						filters = v;
						debouncedDestinationsFilters();
					}
				}
				okCallback={() => filtersDialog.close()}
			/>
		</div>
		<form method="dialog" class="modal-backdrop">
			<button>close</button>
		</form>
	</dialog>
</div>

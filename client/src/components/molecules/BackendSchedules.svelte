<script lang="ts">
	import dayjs from 'dayjs';
	import {
		autocompleteBackendStation,
		fetchBackendDestinations
	} from '$lib/remote/backend-schedules.remote';
	import BackendTripsResults from './BackendTripsResults.svelte';

	const today = dayjs();
	const nextWeek = today.add(1, 'week');
	const format = (d: dayjs.Dayjs) => d.format('YYYY-MM-DD');

	let stop: { id: number; name: string } | undefined = $state(undefined);
	let from: string = $state(format(today));

	// Station autocomplete state
	let query = $state('');
	let autocompleteOptions: { id: number; name: string }[] = $state([]);
	let autocompleteTimer: any;

	const debounce = () => {
		stop = undefined;
		clearTimeout(autocompleteTimer);
		autocompleteTimer = setTimeout(async () => {
			if (query.length >= 2) {
				autocompleteOptions = await autocompleteBackendStation(query);
			} else {
				autocompleteOptions = [];
			}
		}, 200);
	};

	let tripsPromise = $derived.by(() => {
		if (stop === undefined || from === undefined) {
			return undefined;
		}
		return fetchBackendDestinations({ from: stop.id, date: from });
	});
</script>

<div class="mx-auto flex max-w-4xl flex-col gap-3 p-3">
	<div class="bg-base-300 p-3">
		<fieldset class="fieldset">
			<!-- Station autocomplete -->
			<div class="flex max-w-80 flex-col">
				<label class="flex flex-col gap-2">
					<span class="text-sm font-semibold">Gare de départ</span>
					<input
						type="text"
						bind:value={query}
						oninput={debounce}
						class="input w-full pl-2 text-base-content input-info"
					/>
				</label>
				{#if stop === undefined && autocompleteOptions.length > 0}
					<ul class="flex flex-col items-start rounded-b-lg bg-base-100 p-2">
						{#each autocompleteOptions as option (option.id)}
							<li class="p-0.5 hover:bg-base-300">
								<button
									class="text-start"
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

			<!-- Date picker -->
			<div class="flex flex-col items-start justify-stretch gap-2">
				<label for="backend-select-from-date" class="text-sm font-semibold">Date de départ</label>
				<input
					type="date"
					class="input pl-2"
					id="backend-select-from-date"
					bind:value={from}
					min={format(today)}
					max={format(nextWeek)}
				/>
			</div>
		</fieldset>
	</div>

	{#if tripsPromise !== undefined}
		{#await tripsPromise}
			<span class="loading loading-xl self-center loading-dots pt-3"></span>
		{:then result}
			{#if result.origin !== null}
				<BackendTripsResults origin={result.origin} destinations={result.destinations} />
			{:else}
				<p class="text-warning">Gare de départ introuvable dans la base de données.</p>
			{/if}
		{/await}
	{/if}
</div>

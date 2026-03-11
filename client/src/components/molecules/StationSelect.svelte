<script lang="ts">
	import {
		autocompleteStation,
		type AutocompleteStation,
		type City
	} from '$lib/remote/destinations.remote';

	let { city = $bindable(), label }: { city: City | undefined; label: string } = $props();

	let query = $state('');
	let options: AutocompleteStation[] = $state([]);

	let timer: any;

	const debounce = () => {
		city = undefined;
		clearTimeout(timer);

		timer = setTimeout(async () => {
			options = await autocompleteStation(query);
		}, 200);
	};
</script>

<div class="flex max-w-80 flex-col">
	<label class="flex flex-col gap-2">
		<span class="text-sm font-semibold">{label}</span>
		<input
			type="text"
			bind:value={query}
			oninput={debounce}
			class="input w-full pl-2 text-base-content input-info"
		/>
	</label>
	{#if city === undefined && options.length > 0}
		<ul class="flex flex-col items-start rounded-b-lg bg-base-100 p-2">
			{#each options as option (option.id)}
				<li class="p-0.5 hover:bg-base-300">
					<button
						class="text-start"
						onclick={() => {
							city = { ...option, country: '', lat: 0, lon: 0 } as City;
							query = option.name;
						}}
					>
						{option.name}
					</button>
				</li>
			{/each}
		</ul>
	{/if}
</div>

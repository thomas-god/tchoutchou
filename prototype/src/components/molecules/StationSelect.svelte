<script lang="ts">
	import { autocompleteStation, type Station } from '$lib/station.remote';

	let { station = $bindable(), label }: { station: Station | undefined; label: string } = $props();

	let query = $state('');
	let options: Station[] = $state([]);

	let timer: any;

	const debounce = () => {
		station = undefined;
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
	{#if station === undefined && options.length > 0}
		<ul class="flex flex-col items-start rounded-b-lg bg-base-100 p-2">
			{#each options as option (option.id)}
				<li class="p-0.5 hover:bg-base-300">
					<button
						class="text-start"
						onclick={() => {
							station = option;
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

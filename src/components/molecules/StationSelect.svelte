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

<div class="flex flex-col">
	<label class="flex flex-row items-center gap-3">
		<span>{label}</span>
		<input
			type="text"
			bind:value={query}
			oninput={debounce}
			class="input text-base-content input-info"
		/>
	</label>
	{#if station === undefined}
		<ul>
			{#each options as option (option.id)}
				<li>
					<button
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

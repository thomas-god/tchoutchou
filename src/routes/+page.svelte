<script lang="ts">
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
</script>

{#each data.journeys as journey}
	<div class="m-3 bg-amber-200 p-3">
		<h2>
			{journey.duration} - {journey.transfers} transfers
		</h2>
		<ul class="px-3">
			{#each journey.sections as section}
				<li class="list-disc">
					{section.from} -> {section.to} ({section.duration} en {section.mode})
				</li>
			{/each}
		</ul>
	</div>
{/each}

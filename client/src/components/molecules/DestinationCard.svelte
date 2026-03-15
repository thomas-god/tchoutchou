<script lang="ts">
	import { displayDuration } from '$lib';
	import type { DestinationResult } from '$lib/remote/destinations.remote';

	interface Props {
		destination: DestinationResult;
		originName?: string;
		onClose?: () => void;
	}

	let { destination, originName, onClose }: Props = $props();

	// Placeholder values until added to DestinationResult
	const description = 'Une destination incontournable aux mille et une facettes.';
	const weather = 'Ensoleillé';
	const bestPeriod = "Toute l'année";
	const activities = ['Culturel', 'Festif'];

	const photos = $derived([
		`https://picsum.photos/seed/${destination.station.id}a/400/200`,
		`https://picsum.photos/seed/${destination.station.id}b/400/200`
	]);

	const connectionLabel = $derived(
		destination.connections === 0
			? 'direct'
			: destination.connections === 1
				? '1 correspondance'
				: `${destination.connections} correspondances`
	);
</script>

<div class="card relative overflow-hidden bg-base-100 shadow-sm">
	{#if onClose}
		<button
			class="btn absolute top-2 right-2 z-10 btn-circle bg-base-100/80 backdrop-blur-sm btn-sm"
			aria-label="Fermer"
			onclick={onClose}>✕</button
		>
	{/if}
	<!-- Photos -->
	<div class="grid h-32 grid-cols-2 gap-0.5">
		{#each photos as src, i (i)}
			<img {src} alt="Photo de {destination.station.name}" class="h-full w-full object-cover" />
		{/each}
	</div>

	<div class="card-body gap-3 p-4">
		<!-- Title & country -->
		<div>
			<h2 class="card-title text-xl leading-tight">{destination.station.name}</h2>
			<p class="mt-0.5 flex items-center gap-1 text-sm text-base-content/60">
				<img src="/icons/city.svg" alt="" class="h-3.5 w-3.5 opacity-60" />
				{destination.station.country}
			</p>
		</div>

		<!-- Description -->
		<p class="text-sm text-base-content/80 italic">{description}</p>

		<!-- Travel time chip -->
		<div
			class="flex items-center gap-1.5 rounded-full bg-primary px-3 py-1.5 text-sm font-medium text-primary-content"
		>
			<img src="/icons/locomotive.svg" alt="" class="h-3.5 w-3.5 shrink-0 invert" />
			<span
				>{displayDuration(destination.duration)} depuis {originName ?? 'votre ville'} ({connectionLabel})</span
			>
		</div>

		<!-- Weather & best period -->
		<div class="flex items-center gap-4 text-sm text-base-content/70">
			<span class="flex items-center gap-1">
				<span class="text-base">🌤</span>
				{weather}
			</span>
			<span class="flex items-center gap-1">
				<span class="text-base">📅</span>
				{bestPeriod}
			</span>
		</div>

		<!-- Activity tags -->
		<div class="flex flex-wrap gap-1.5">
			{#each activities as activity (activity)}
				<span class="badge badge-outline badge-sm">{activity}</span>
			{/each}
		</div>
	</div>
</div>

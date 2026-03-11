<script lang="ts">
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';

	let { children } = $props();
</script>

<svelte:head><link rel="icon" href={favicon} /></svelte:head>
<nav class="flex gap-4 border-b border-base-300 bg-base-100 px-6 py-3">
	<a href="/" class="font-semibold hover:text-primary">Home</a>
	<a href="/map" class="hover:text-primary">Map</a>
</nav>
{@render children()}

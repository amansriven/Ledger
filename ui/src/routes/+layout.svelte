<script lang="ts">
	import '../app.css';
	import favicon from '#lib/assets/favicon.svg';
	import type { LayoutProps } from './$types';

	let { children }: LayoutProps = $props();
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<title>Ledger</title>
</svelte:head>

<header>
	<a class="brand" href="/">Ledger</a>
	<span class="muted">trace</span>
</header>

<main>
	{@render children()}
</main>

<style>
	header {
		display: flex;
		align-items: baseline;
		gap: 8px;
		padding: 12px 24px;
		border-bottom: 1px solid var(--border);
		background: var(--surface);
	}
	.brand {
		font-weight: 700;
		color: var(--text);
		letter-spacing: -0.01em;
	}
	main {
		padding: 20px 24px 48px;
		max-width: 1400px;
		margin: 0 auto;
	}
	@media (max-width: 640px) {
		header,
		main {
			padding-left: 16px;
			padding-right: 16px;
		}
	}
</style>

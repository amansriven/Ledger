<script lang="ts">
	import '../app.css';
	import favicon from '#lib/assets/favicon.svg';
	import type { LayoutProps } from './$types';

	let { children }: LayoutProps = $props();

	// Set only when building the hosted demo image (deploy/Dockerfile.server).
	const demo = import.meta.env.VITE_LEDGER_DEMO === '1';
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
	<title>Ledger</title>
</svelte:head>

<header>
	<a class="brand" href="/">Ledger</a>
	<span class="muted">trace</span>
</header>

{#if demo}
	<aside class="demo">
		A demo store of sample runs, read-only. To see your own, <code>pip install ledger-runtime</code>
		and run <code>ledger ui</code>.
		<a href="https://github.com/amansriven/Ledger">Ledger on GitHub</a>
	</aside>
{/if}

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
	.demo {
		padding: 8px 24px;
		border-bottom: 1px solid var(--border);
		background: var(--band);
		color: var(--muted);
		font-size: 13px;
	}
	.demo code {
		color: var(--text);
	}
	.demo a {
		margin-left: 8px;
	}
	main {
		padding: 20px 24px 48px;
		max-width: 1400px;
		margin: 0 auto;
	}
	@media (max-width: 640px) {
		header,
		.demo,
		main {
			padding-left: 16px;
			padding-right: 16px;
		}
	}
</style>

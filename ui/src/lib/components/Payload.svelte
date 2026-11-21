<script lang="ts">
	import { api } from '#lib/api.js';
	import { decode, decodeBytes } from '#lib/events.js';
	import type { Payload } from '#lib/gen/events_pb.js';

	let { payload, title }: { payload: Payload | undefined; title: string } = $props();

	let fetched = $state<unknown>(undefined);
	let loading = $state(false);
	let error = $state('');
	const decoded = $derived(decode(payload));

	async function load(sha256: string) {
		loading = true;
		try {
			const res = await api.getPayload({ sha256 });
			fetched = decodeBytes(res.payload?.json ?? new Uint8Array());
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
		loading = false;
	}

	function show(v: unknown): string {
		return typeof v === 'string' ? v : JSON.stringify(v, null, 2);
	}
</script>

{#if decoded.state !== 'none'}
	<details open>
		<summary>{title}</summary>
		{#if decoded.state === 'ok'}
			<pre>{show(decoded.value)}</pre>
		{:else if fetched !== undefined}
			<pre>{show(fetched)}</pre>
		{:else}
			<p class="muted">
				Too large to send with the run.
				<button onclick={() => load(decoded.sha256)} disabled={loading}>{loading ? 'Loading…' : 'Load it'}</button>
			</p>
			{#if error}<p class="error-box">{error}</p>{/if}
		{/if}
	</details>
{/if}

<style>
	details {
		margin-top: 12px;
	}
	summary {
		cursor: pointer;
		color: var(--muted);
		font-size: 12px;
		margin-bottom: 4px;
	}
	pre {
		margin: 0;
		padding: 10px 12px;
		background: var(--surface-2);
		border-radius: 6px;
		max-height: 360px;
		overflow: auto;
		white-space: pre-wrap;
		word-break: break-word;
	}
	button {
		font: inherit;
		margin-left: 8px;
	}
</style>

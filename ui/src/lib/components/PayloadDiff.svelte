<script lang="ts">
	import { api } from '#lib/api.js';
	import { decode, decodeBytes } from '#lib/events.js';
	import type { Payload } from '#lib/gen/events_pb.js';
	import { asText, lineDiff } from '#lib/textdiff.js';

	let {
		a,
		b,
		title,
		readable = asText
	}: { a?: Payload; b?: Payload; title: string; readable?: (v: unknown) => string } = $props();

	// Values, fetching payloads that were too large to come with the run.
	let values = $state<[unknown, unknown] | undefined>();
	let error = $state('');

	$effect(() => {
		const pair = [a, b] as const;
		values = undefined;
		error = '';
		Promise.all(
			pair.map(async (p) => {
				const d = decode(p);
				if (d.state === 'ok') return d.value;
				if (d.state === 'none') return undefined;
				const res = await api.getPayload({ sha256: d.sha256 });
				return decodeBytes(res.payload?.json ?? new Uint8Array());
			})
		)
			.then((v) => (values = [v[0], v[1]]))
			.catch((e) => (error = e instanceof Error ? e.message : String(e)));
	});

	const text = (v: unknown) => (v === undefined ? '' : readable(v));
	const ops = $derived(values ? lineDiff(text(values[0]), text(values[1])) : []);
</script>

<div class="pd">
	<div class="title muted">{title}</div>
	{#if error}
		<p class="error-box">{error}</p>
	{:else if !values}
		<p class="muted">Loading…</p>
	{:else}
		<pre>{#each ops as o, i (i)}<span class={o.op}>{o.op === 'del' ? '−' : o.op === 'add' ? '+' : ' '} {o.text}
</span>{/each}</pre>
	{/if}
</div>

<style>
	.title {
		font-size: 12px;
		margin: 10px 0 4px;
	}
	pre {
		margin: 0;
		padding: 8px 0;
		background: var(--surface-2);
		border-radius: 6px;
		max-height: 420px;
		overflow: auto;
		white-space: pre-wrap;
		word-break: break-word;
	}
	span {
		display: block;
		padding: 0 12px;
	}
	.del {
		background: color-mix(in srgb, var(--error) 14%, transparent);
	}
	.add {
		background: color-mix(in srgb, var(--ok) 14%, transparent);
	}
	.same {
		color: var(--muted);
	}
</style>

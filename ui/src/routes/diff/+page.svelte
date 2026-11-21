<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { ConnectError } from '@connectrpc/connect';
	import { api } from '#lib/api.js';
	import PayloadDiff from '#lib/components/PayloadDiff.svelte';
	import { diffRuns, payloadsOf, type DiffRow, type RowStatus } from '#lib/diff.js';
	import { MODE_NAMES, shortId, summary } from '#lib/events.js';
	import type { Event } from '#lib/gen/events_pb.js';
	import type { RunSummary } from '#lib/gen/trace_service_pb.js';
	import { asText, llmText } from '#lib/textdiff.js';

	const STATUS: Record<RowStatus, { label: string; cls: string }> = {
		same: { label: 'same', cls: '' },
		changed: { label: 'changed', cls: 'unfinished' },
		edited: { label: 'edited', cls: 'diverged' },
		rerun: { label: 'rerun', cls: 'diverged' },
		diverged: { label: 'diverged', cls: 'diverged' },
		onlyA: { label: 'only in A', cls: 'error' },
		onlyB: { label: 'only in B', cls: 'ok' }
	};

	let runs = $state.raw<RunSummary[]>([]);
	let a = $state.raw<{ summary: RunSummary; events: Event[] } | undefined>();
	let b = $state.raw<{ summary: RunSummary; events: Event[] } | undefined>();
	let error = $state('');
	let onlyChanges = $state(true);
	let open = $state<string | undefined>();

	const bId = $derived(page.url.searchParams.get('b') ?? '');
	const aParam = $derived(page.url.searchParams.get('a') ?? '');
	// Without ?a=, B is compared with the run it replays or forks.
	const aId = $derived(aParam || (b?.summary.runId === bId ? b.summary.sourceRunId : ''));

	$effect(() => {
		api.listRuns({}).then((r) => (runs = r.runs)).catch(() => {});
	});

	async function load(id: string) {
		const res = await api.getRun({ runId: id });
		return { summary: res.summary!, events: res.events };
	}

	$effect(() => {
		const id = bId;
		b = undefined;
		error = '';
		if (id) load(id).then((r) => (b = r)).catch((e) => (error = ConnectError.from(e).message));
	});
	$effect(() => {
		const id = aId;
		a = undefined;
		if (id) load(id).then((r) => (a = r)).catch((e) => (error = ConnectError.from(e).message));
	});

	const d = $derived(a && b ? diffRuns(a.events, b.events) : undefined);
	const differences = $derived(d ? d.rows.filter((r) => r.status !== 'same').length : 0);
	const rows = $derived(d ? (onlyChanges && differences ? d.rows.filter((r) => r.status !== 'same') : d.rows) : []);

	function pick(which: 'a' | 'b', id: string) {
		const q = new URLSearchParams(page.url.search);
		if (which === 'a') q.set('a', id);
		else {
			q.set('b', id);
			if (!aParam) q.delete('a');
		}
		goto(`?${q}`, { replaceState: true });
	}

	function swap() {
		if (aId && bId) goto(`?a=${encodeURIComponent(bId)}&b=${encodeURIComponent(aId)}`);
	}

	const outcomeOf = (ev: Event | undefined) => (ev ? summary(ev, 60) : '');
	const readable = (r: DiffRow) => (r.kind === 'llmCall' ? llmText : asText);
	const ending = (e: Event[] | undefined) => e?.findLast((x) => x.body.case === 'runFinished');
	const endPayload = (e: Event[] | undefined) => {
		const fin = ending(e);
		return fin?.body.case === 'runFinished' ? fin.body.value.output : undefined;
	};
</script>

<svelte:head><title>Diff · Ledger</title></svelte:head>

<nav class="crumbs muted"><a href="/">Runs</a> / diff</nav>

<div class="pickers panel">
	<label>
		<span class="muted">A</span>
		<select value={aId} onchange={(e) => pick('a', e.currentTarget.value)}>
			<option value="" disabled>Pick a run</option>
			{#each runs as r (r.runId)}
				<option value={r.runId}>{shortId(r.runId)} · {MODE_NAMES[r.mode]}</option>
			{/each}
		</select>
		{#if aId}<a class="mono small" href="/runs/{aId}">open</a>{/if}
	</label>
	<button class="swap" onclick={swap} disabled={!aId || !bId} title="Swap A and B">⇄</button>
	<label>
		<span class="muted">B</span>
		<select value={bId} onchange={(e) => pick('b', e.currentTarget.value)}>
			<option value="" disabled>Pick a run</option>
			{#each runs as r (r.runId)}
				<option value={r.runId}>{shortId(r.runId)} · {MODE_NAMES[r.mode]}</option>
			{/each}
		</select>
		{#if bId}<a class="mono small" href="/runs/{bId}">open</a>{/if}
	</label>
</div>

{#if error}
	<p class="error-box">{error}</p>
{:else if !bId}
	<p class="muted">Pick two runs to compare, or open a replay or fork and compare it with its recording.</p>
{:else if b && !aId}
	<p class="muted">Run <span class="mono">{shortId(bId)}</span> is a recording; pick a run to compare it with.</p>
{:else if !d}
	<p class="muted">Loading…</p>
{:else}
	<div class="summary">
		{#each Object.entries(d.counts) as [status, n] (status)}
			{#if n > 0}
				<span class="badge {STATUS[status as RowStatus].cls}">{n} {STATUS[status as RowStatus].label}</span>
			{/if}
		{/each}
		<span class="badge {d.sameEnding ? 'ok' : 'error'}">{d.sameEnding ? 'same output' : 'different output'}</span>
		<label class="toggle muted">
			<input type="checkbox" bind:checked={onlyChanges} disabled={!differences} /> only differences
		</label>
	</div>

	<div class="panel table-wrap">
		<table>
			<thead>
				<tr><th>Change</th><th>Node</th><th>Call</th><th>A</th><th>B</th></tr>
			</thead>
			<tbody>
				{#each rows as r (r.key)}
					{@const st = STATUS[r.status]}
					{@const p = payloadsOf(r)}
					<tr class="row {r.status}" class:open={open === r.key} onclick={() => (open = open === r.key ? undefined : r.key)}>
						<td>
							<span class="badge {st.cls}">{r.reason ?? st.label}</span>
						</td>
						<td class="mono">{r.node}</td>
						<td class="mono"><span class="kind {r.kind}"></span>{r.label}</td>
						<td class="out" class:muted={r.status === 'same'}>{outcomeOf(r.a)}</td>
						<td class="out" class:muted={r.status === 'same'}>{outcomeOf(r.b)}</td>
					</tr>
					{#if open === r.key}
						<tr class="detail">
							<td colspan="5">
								<div class="mono muted small">{r.key} · A #{r.a?.index ?? '–'} · B #{r.b?.index ?? '–'}</div>
								{#if r.status === 'onlyA' || r.status === 'onlyB'}
									<PayloadDiff a={p.request[0]} b={p.request[1]} title="Request" readable={readable(r)} />
								{:else}
									{#if r.requestChanged}
										<PayloadDiff a={p.request[0]} b={p.request[1]} title="Request (A → B)" readable={readable(r)} />
									{:else}
										<p class="muted small">Same request.</p>
									{/if}
									{#if r.outcomeChanged}
										<PayloadDiff a={p.outcome[0]} b={p.outcome[1]} title="Outcome (A → B)" readable={readable(r)} />
									{:else}
										<p class="muted small">Same outcome.</p>
									{/if}
								{/if}
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	</div>

	{#if !d.sameEnding}
		<section class="panel ending">
			<h2>Output</h2>
			<PayloadDiff a={endPayload(a?.events)} b={endPayload(b?.events)} title="A → B" />
		</section>
	{/if}
{/if}

<style>
	.crumbs {
		margin-bottom: 12px;
		font-size: 13px;
	}
	.pickers {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 10px 16px;
		padding: 12px 16px;
		margin-bottom: 16px;
	}
	.pickers label {
		display: flex;
		align-items: center;
		gap: 8px;
		min-width: 0;
	}
	select {
		font: inherit;
		font-family: var(--mono);
		font-size: 12.5px;
		max-width: 260px;
		padding: 4px 6px;
		background: var(--surface-2);
		color: var(--text);
		border: 1px solid var(--border);
		border-radius: 6px;
	}
	.swap {
		font-size: 16px;
		background: none;
		border: 1px solid var(--border);
		border-radius: 6px;
		color: var(--text);
		cursor: pointer;
		padding: 2px 10px;
	}
	.small {
		font-size: 12px;
	}
	.summary {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
		align-items: center;
		margin-bottom: 12px;
	}
	.toggle {
		margin-left: auto;
		font-size: 13px;
		display: flex;
		gap: 6px;
		align-items: center;
	}
	.table-wrap {
		overflow-x: auto;
	}
	table {
		width: 100%;
		border-collapse: collapse;
	}
	th,
	td {
		text-align: left;
		padding: 6px 12px;
		vertical-align: top;
	}
	th {
		font-weight: 500;
		color: var(--muted);
		font-size: 12px;
		border-bottom: 1px solid var(--border);
		white-space: nowrap;
	}
	tr.row {
		cursor: pointer;
		border-top: 1px solid var(--border);
	}
	tr.row:hover,
	tr.open {
		background: var(--surface-2);
	}
	td:nth-child(-n + 3) {
		white-space: nowrap;
	}
	.out {
		min-width: 200px;
		word-break: break-word;
	}
	tr.onlyA .out:nth-child(5),
	tr.onlyB .out:nth-child(4) {
		color: var(--muted);
	}
	tr.detail td {
		padding-bottom: 16px;
		background: var(--surface-2);
	}
	.kind {
		display: inline-block;
		width: 8px;
		height: 8px;
		border-radius: 2px;
		margin-right: 6px;
		background: var(--c);
	}
	.kind.llmCall {
		--c: var(--llm);
	}
	.kind.toolCall {
		--c: var(--tool);
	}
	.kind.effect {
		--c: var(--effect);
	}
	.kind.nondet {
		--c: var(--nondet);
	}
	.ending {
		margin-top: 16px;
		padding: 12px 16px;
	}
</style>

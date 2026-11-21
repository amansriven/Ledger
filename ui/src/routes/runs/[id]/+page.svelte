<script lang="ts">
	import { page } from '$app/state';
	import { Code, ConnectError } from '@connectrpc/connect';
	import { api } from '#lib/api.js';
	import CallDetail from '#lib/components/CallDetail.svelte';
	import Timeline from '#lib/components/Timeline.svelte';
	import {
		MODE_NAMES,
		STATUS_NAMES,
		countsAsCall,
		isCall,
		label,
		ms,
		shortId,
		statusOf,
		summary
	} from '#lib/events.js';
	import { EffectCall_Phase, type Event } from '#lib/gen/events_pb.js';
	import { formatMs, layout, type Bar } from '#lib/timeline.js';

	const runId = $derived(page.params.id ?? '');

	let events = $state.raw<Event[]>([]);
	let watching = $state(false);
	let error = $state('');
	let missing = $state(false);
	let selected = $state<Bar | undefined>();

	// Follow the run: its backlog, then each event as it's logged, until it
	// finishes. Events are added once per frame, not one by one.
	$effect(() => {
		const id = runId;
		const abort = new AbortController();
		let buffer: Event[] = [];
		let frame = 0;
		const flush = () => {
			frame = 0;
			events = [...events, ...buffer];
			buffer = [];
		};
		events = [];
		selected = undefined;
		error = '';
		missing = false;
		watching = true;
		(async () => {
			try {
				for await (const res of api.watchRun({ runId: id, fromIndex: 0n }, { signal: abort.signal })) {
					if (res.event) buffer.push(res.event);
					frame ||= requestAnimationFrame(flush);
				}
			} catch (e) {
				if (abort.signal.aborted) return;
				if (ConnectError.from(e).code === Code.NotFound) missing = true;
				else error = ConnectError.from(e).message;
			}
			if (frame) cancelAnimationFrame(frame);
			if (buffer.length) flush();
			watching = false;
		})();
		return () => {
			abort.abort();
			if (frame) cancelAnimationFrame(frame);
		};
	});

	const started = $derived(events[0]?.body.case === 'runStarted' ? events[0].body.value : undefined);
	const status = $derived(STATUS_NAMES[statusOf(events)]);
	const tl = $derived(layout(events));
	const calls = $derived(events.filter(isCall));
	const counted = $derived(events.filter(countsAsCall));
	const live = $derived(counted.filter((e) => !e.fromLog).length);
	const barOf = $derived(new Map(tl.lanes.flatMap((l) => l.bars.flatMap((b) => b.events.map((i) => [i, b] as const)))));
	const finished = $derived(events.findLast((e) => e.body.case === 'runFinished'));
	const failure = $derived(finished?.body.case === 'runFinished' ? finished.body.value.error : undefined);
</script>

<svelte:head><title>{shortId(runId)} · Ledger</title></svelte:head>

<nav class="crumbs muted"><a href="/">Runs</a> / <span class="mono">{runId}</span></nav>

{#if missing}
	<p class="error-box">There's no run <code>{runId}</code> in this store.</p>
{:else}
	<div class="title">
		<h1 class="mono">{shortId(runId)}</h1>
		{#if started}<span class="badge">{MODE_NAMES[started.mode]}</span>{/if}
		{#if events.length}
			<span class="badge {status}">{status === 'unfinished' && watching ? 'running' : status}</span>
		{/if}
		{#if watching && events.length}<span class="live-dot" title="following new events">live</span>{/if}
	</div>
	<div class="facts muted">
		{#if started?.sourceRunId}
			<span>{MODE_NAMES[started.mode]} of <a class="mono" href="/runs/{started.sourceRunId}">{shortId(started.sourceRunId)}</a></span>
		{/if}
		{#if started?.graph}<span class="mono">{started.graph}</span>{/if}
		<span>{counted.length} calls, {live} live</span>
		{#if tl.end}<span>{formatMs(tl.end)}</span>{/if}
		{#if events[0]}<span>{new Date(ms(events[0].recordedAt)).toLocaleString()}</span>{/if}
	</div>

	{#if error}<p class="error-box">Lost the stream: {error}</p>{/if}
	{#if failure}
		<p class="error-box"><strong>{failure.type}</strong>: {failure.message}</p>
	{/if}

	<div class="layout" class:with-detail={selected}>
		<div class="main">
			<section class="panel">
				{#if events.length === 0 && watching}
					<p class="muted pad">Loading…</p>
				{:else}
					<Timeline layout={tl} bind:selected />
				{/if}
			</section>

			<section class="panel calls">
				<h2 class="pad-h">Calls in log order</h2>
				<table>
					<thead>
						<tr><th class="num">#</th><th>Kind</th><th>Node</th><th>Name</th><th>Outcome</th></tr>
					</thead>
					<tbody>
						{#each calls as ev (ev.index)}
							{@const bar = barOf.get(Number(ev.index))}
							<tr
								class:replayed={ev.fromLog}
								class:selected={bar && selected === bar}
								onclick={() => (selected = bar)}
							>
								<td class="num mono muted">{ev.index}</td>
								<td><span class="kind {ev.body.case}">{ev.body.case?.replace('Call', '')}</span></td>
								<td class="mono">{ev.step?.node}</td>
								<td class="mono">{label(ev)}</td>
								<td class="outcome">
									{#if bar?.diverged && bar.events[0] === Number(ev.index)}<span class="badge diverged">{bar.diverged}</span>{/if}
									{#if ev.body.case === 'effect' && ev.body.value.phase === EffectCall_Phase.PENDING && bar?.phase !== 'pending'}
										<span class="muted">pending: logged before the call, settled at #{bar?.events.at(-1)}</span>
									{:else}
										{summary(ev)}
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</section>
		</div>
		{#if selected}
			<CallDetail bar={selected} {events} onclose={() => (selected = undefined)} />
		{/if}
	</div>
{/if}

<style>
	.crumbs {
		margin-bottom: 8px;
		font-size: 13px;
	}
	.title {
		display: flex;
		align-items: center;
		gap: 10px;
		flex-wrap: wrap;
	}
	.facts {
		display: flex;
		flex-wrap: wrap;
		gap: 4px 18px;
		margin: 6px 0 16px;
		font-size: 13px;
	}
	.live-dot {
		font-size: 12px;
		color: var(--ok);
	}
	.live-dot::before {
		content: '';
		display: inline-block;
		width: 8px;
		height: 8px;
		border-radius: 50%;
		background: var(--ok);
		margin-right: 6px;
		animation: pulse 1.4s ease-in-out infinite;
	}
	@keyframes pulse {
		50% {
			opacity: 0.3;
		}
	}
	@media (prefers-reduced-motion: reduce) {
		.live-dot::before {
			animation: none;
		}
	}
	.layout {
		display: grid;
		grid-template-columns: minmax(0, 1fr);
		gap: 16px;
		align-items: start;
	}
	.layout.with-detail {
		grid-template-columns: minmax(0, 1fr) minmax(320px, 420px);
	}
	@media (max-width: 960px) {
		.layout.with-detail {
			grid-template-columns: minmax(0, 1fr);
		}
	}
	.main {
		display: grid;
		grid-template-columns: minmax(0, 1fr);
		gap: 16px;
		min-width: 0;
	}
	.pad {
		padding: 16px;
		margin: 0;
	}
	.pad-h {
		padding: 12px 12px 0;
	}
	.calls {
		overflow-x: auto;
	}
	table {
		width: 100%;
		border-collapse: collapse;
	}
	th,
	td {
		text-align: left;
		padding: 5px 12px;
		white-space: nowrap;
	}
	th {
		font-weight: 500;
		color: var(--muted);
		font-size: 12px;
		border-bottom: 1px solid var(--border);
	}
	tbody tr {
		cursor: pointer;
	}
	tbody tr:hover {
		background: var(--surface-2);
	}
	tr.selected {
		background: color-mix(in srgb, var(--focus) 12%, transparent) !important;
	}
	tr.replayed td:not(.outcome) {
		color: var(--muted);
	}
	.num {
		text-align: right;
	}
	.outcome {
		white-space: normal;
		min-width: 240px;
	}
	.kind {
		font-size: 12px;
	}
	.kind::before {
		content: '';
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
</style>

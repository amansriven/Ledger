<script lang="ts">
	import { page } from '$app/state';
	import { Code, ConnectError } from '@connectrpc/connect';
	import { api } from '#lib/api.js';
	import Timeline from '#lib/components/Timeline.svelte';
	import {
		candidates,
		describe,
		parseDiagnosis,
		passText,
		reranCells,
		type Diagnosis
	} from '#lib/diagnosis.js';
	import { ms, shortId } from '#lib/events.js';
	import type { Event } from '#lib/gen/events_pb.js';
	import { layout, type Bar } from '#lib/timeline.js';

	const runId = $derived(page.params.id ?? '');

	let events = $state.raw<Event[]>([]);
	let saved = $state.raw<{ id: bigint; at: number; d: Diagnosis | undefined; error: string }[]>([]);
	let chosen = $state<bigint | undefined>();
	let error = $state('');
	let missing = $state(false);
	let loaded = $state(false);
	let selected = $state<Bar | undefined>();

	$effect(() => {
		const id = runId;
		loaded = false;
		error = '';
		missing = false;
		Promise.all([api.getRun({ runId: id }), api.listDiagnoses({ runId: id })])
			.then(([run, list]) => {
				events = run.events;
				saved = list.diagnoses.map((x) => {
					try {
						return { id: x.id, at: ms(x.createdAt), d: parseDiagnosis(x.json), error: '' };
					} catch (e) {
						return { id: x.id, at: ms(x.createdAt), d: undefined, error: String(e) };
					}
				});
				chosen = saved.at(-1)?.id;
			})
			.catch((e) => {
				const err = ConnectError.from(e);
				if (err.code === Code.NotFound) missing = true;
				else error = err.message;
			})
			.finally(() => (loaded = true));
	});

	const current = $derived(saved.find((s) => s.id === chosen));
	const d = $derived(current?.d);
	const cells = $derived(d ? candidates(events, d) : []);
	const tl = $derived(layout(events));
	const guiltyKeys = $derived(d?.steps.map((s) => s.key) ?? []);
	const narrowedKeys = $derived(d?.narrowed.map((s) => s.key) ?? []);
	const ordinal = (n: number) => (n === 1 ? 'call' : 'calls');
	const money = (v: number) => (v < 0.01 && v > 0 ? `$${v.toFixed(4)}` : `$${v.toFixed(2)}`);
</script>

<svelte:head><title>Blame {shortId(runId)} · Ledger</title></svelte:head>

<nav class="crumbs muted">
	<a href="/">Runs</a> / <a class="mono" href="/runs/{runId}">{shortId(runId)}</a> / blame
</nav>

{#if missing}
	<p class="error-box">There's no run <code>{runId}</code> in this store.</p>
{:else if error}
	<p class="error-box">{error}</p>
{:else if !loaded}
	<p class="muted">Loading…</p>
{:else if saved.length === 0}
	<div class="panel pad">
		<h1>No diagnosis yet</h1>
		<p class="muted">
			Run <code>ledger diagnose {runId} --check module:passed</code> to find the call that broke this run.
			The diagnosis is saved in the store and shows up here.
		</p>
	</div>
{:else}
	<div class="title">
		<h1>Blame</h1>
		{#if saved.length > 1}
			<select bind:value={chosen} aria-label="Diagnosis">
				{#each saved as s (s.id)}
					<option value={s.id}>diagnosis {s.id} · {new Date(s.at).toLocaleString()}</option>
				{/each}
			</select>
		{:else if current}
			<span class="muted">{new Date(current.at).toLocaleString()}</span>
		{/if}
	</div>

	{#if current?.error}
		<p class="error-box">Can't read this diagnosis: {current.error}</p>
	{:else if d}
		<section class="panel verdict {d.status}">
			{#if d.status === 'found'}
				<p class="headline">
					{#each d.steps as s, i (s.key)}
						{i > 0 ? (i === d.steps.length - 1 ? ' and ' : ', ') : ''}<span class="mono">#{s.index}</span>, the
						{s.kind === 'llm_call' ? 'LLM call' : s.kind === 'tool_call' ? 'tool call' : `${s.label} read`}
						in <span class="mono">{s.node}</span>,
					{/each}
					{d.steps.length === 1 ? 'caused the failure.' : 'together caused the failure.'}
				</p>
				<p class="muted">
					With {d.steps.length === 1 ? 'its recorded outcome' : 'their recorded outcomes'} kept, the run fails even
					when every later call is drawn again; draw {d.steps.length === 1 ? 'it' : 'them'} again and it passes.
				</p>
				{#each d.steps as s (s.key)}
					<p class="outcome"><span class="muted">Recorded outcome of #{s.index}:</span> <span class="mono">{s.summary}</span></p>
				{/each}
			{:else if d.status === 'no_step'}
				<p class="headline">No recorded call is to blame.</p>
				<p class="muted">
					Rerunning every candidate call still fails, so the cause is in the code or the input, or the failure comes
					back each time its call is drawn again.
				</p>
			{:else if d.status === 'budget'}
				<p class="headline">The budget ran out before the search finished.</p>
				<p class="muted">
					{#if d.narrowed.length}
						The call that broke the run is one of the {d.narrowed.length} outlined below,
						most likely <span class="mono">#{d.narrowed[0].index}</span>.
					{:else}
						It stopped before narrowing anything down.
					{/if}
				</p>
			{:else}
				<p class="headline">The reruns contradicted each other.</p>
				<p class="muted">
					Noisy reruns gave answers that can't all be true.
					{#if d.steps.length}Found so far: {d.steps.map((s) => `#${s.index}`).join(', ')}.{/if}
					Try more repeats.
				</p>
			{/if}
		</section>

		<section class="panel pad">
			<h2>How it was found</h2>
			<p class="muted small">
				Each row is a fork of this run in which the filled calls were drawn again live, along with everything that
				depends on them; the rest replayed from the log. {d.candidates} {ordinal(d.candidates)} could be blamed.
			</p>
			<div class="bisect" style:--n={cells.length}>
				<div class="row header">
					<div class="what"></div>
					<div class="cells">
						{#each cells as c (c.key)}
							<div
								class="cell head {c.kind}"
								class:guilty={c.guilty}
								class:narrowed={c.narrowed}
								title="#{c.index} {c.label} in {c.node}"
							>
								{c.index}
							</div>
						{/each}
					</div>
					<div class="result"></div>
				</div>
				{#each d.counterfactuals as cf, i (i)}
					{@const filled = reranCells(cells, cf)}
					<div class="row">
						<div class="what small">{describe(cf, cells)}</div>
						<div class="cells">
							{#each cells as c, j (c.key)}
								<div class="cell {c.kind}" class:on={filled[j]} class:guilty={c.guilty}></div>
							{/each}
						</div>
						<div class="result small">
							<span class="badge {cf.fixed ? 'ok' : 'error'}">{cf.fixed ? 'fixed' : 'still fails'}</span>
							<span class="muted">{passText(cf)}</span>
							{#if cf.run_ids[0]}
								<a href="/diff?a={runId}&b={cf.run_ids[0]}" title="Compare the fork with this run">diff</a>
							{/if}
						</div>
					</div>
				{/each}
			</div>
		</section>

		<section class="panel">
			<h2 class="pad-h">The failed run</h2>
			<Timeline layout={tl} bind:selected highlight={[...guiltyKeys, ...narrowedKeys]} />
		</section>

		<section class="panel pad facts small">
			<div><span class="muted">Spent</span> {d.spent.forks} forks, {d.spent.live_calls} live calls{#if d.spent.tokens_in || d.spent.tokens_out}, {d.spent.tokens_in + d.spent.tokens_out} tokens{/if}{#if d.settings?.pricing}, {money(d.spent.dollars)}{/if}</div>
			{#if d.settings}
				<div>
					<span class="muted">Settings</span>
					{d.settings.repeats} {d.settings.repeats === 1 ? 'run' : 'runs'} per fork, fixed above {Math.round(d.settings.threshold * 100)}%,
					blaming {d.settings.kinds.join(', ')}
					{#if d.settings.budget}
						· budget {[
							d.settings.budget.forks != null && `${d.settings.budget.forks} forks`,
							d.settings.budget.live_calls != null && `${d.settings.budget.live_calls} live calls`,
							d.settings.budget.dollars != null && money(d.settings.budget.dollars)
						].filter(Boolean).join(', ')}
					{/if}
				</div>
			{/if}
			{#if d.check}<div><span class="muted">Check</span> <span class="mono">{d.check}</span></div>{/if}
		</section>
	{/if}
{/if}

<style>
	.crumbs {
		margin-bottom: 8px;
		font-size: 13px;
	}
	.title {
		display: flex;
		align-items: baseline;
		gap: 12px;
		margin-bottom: 16px;
	}
	select {
		font: inherit;
		font-size: 13px;
		background: var(--surface-2);
		color: var(--text);
		border: 1px solid var(--border);
		border-radius: 6px;
		padding: 2px 6px;
	}
	section {
		margin-bottom: 16px;
	}
	.pad {
		padding: 16px;
	}
	.pad-h {
		padding: 12px 12px 0;
	}
	.small {
		font-size: 13px;
	}
	.verdict {
		padding: 16px 20px;
		border-left: 4px solid var(--muted);
	}
	.verdict.found {
		border-left-color: var(--error);
	}
	.verdict.budget,
	.verdict.inconsistent {
		border-left-color: var(--pending);
	}
	.verdict p {
		margin: 0 0 6px;
	}
	.headline {
		font-size: 16px;
		font-weight: 600;
	}
	.outcome {
		margin-top: 10px !important;
	}
	.bisect {
		overflow-x: auto;
		margin-top: 12px;
	}
	.row {
		display: grid;
		grid-template-columns: 200px auto minmax(220px, 1fr);
		align-items: center;
		gap: 12px;
		padding: 3px 0;
		min-width: max-content;
	}
	.row + .row:not(.header) {
		border-top: 1px dashed var(--border);
	}
	.cells {
		display: grid;
		grid-template-columns: repeat(var(--n), 26px);
		gap: 3px;
	}
	.cell {
		height: 18px;
		border-radius: 3px;
		border: 1px solid var(--border);
		background: var(--surface-2);
	}
	.cell.on {
		background: var(--c);
		border-color: var(--c);
	}
	.cell.llmCall {
		--c: var(--llm);
	}
	.cell.toolCall {
		--c: var(--tool);
	}
	.cell.nondet {
		--c: var(--nondet);
	}
	.cell.head {
		height: 22px;
		background: none;
		border: 1px solid transparent;
		border-bottom: 3px solid var(--c);
		font-family: var(--mono);
		font-size: 11px;
		text-align: center;
		line-height: 18px;
		color: var(--muted);
	}
	.cell.head.guilty {
		color: var(--error);
		font-weight: 700;
		border: 2px solid var(--error);
		border-radius: 4px;
	}
	.cell.head.narrowed {
		border: 1px dashed var(--pending);
		border-bottom: 3px solid var(--c);
		color: var(--text);
	}
	.cell.guilty:not(.head) {
		box-shadow: inset 0 0 0 1px var(--error);
	}
	.result {
		display: flex;
		gap: 8px;
		align-items: center;
		white-space: nowrap;
	}
	.facts {
		display: grid;
		gap: 4px;
	}
</style>

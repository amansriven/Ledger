<script lang="ts">
	import { api } from '#lib/api.js';
	import { MODE_NAMES, STATUS_NAMES, ms, shortId } from '#lib/events.js';
	import type { RunSummary } from '#lib/gen/trace_service_pb.js';
	import { ago, durationMs, groupRuns } from '#lib/runs.js';
	import { formatMs } from '#lib/timeline.js';

	let runs = $state.raw<RunSummary[]>([]);
	let loaded = $state(false);
	let error = $state('');
	let now = $state(Date.now());

	$effect(() => {
		let timer: ReturnType<typeof setTimeout>;
		let stopped = false;
		async function poll() {
			try {
				runs = (await api.listRuns({})).runs;
				error = '';
			} catch (e) {
				error = e instanceof Error ? e.message : String(e);
			}
			loaded = true;
			now = Date.now();
			if (!stopped) timer = setTimeout(poll, 2000);
		}
		poll();
		return () => {
			stopped = true;
			clearTimeout(timer);
		};
	});

	const groups = $derived(groupRuns(runs));
</script>

{#snippet row(r: RunSummary, child: boolean)}
	{@const status = STATUS_NAMES[r.status]}
	{@const duration = durationMs(r)}
	<tr class:child>
		<td class="id">
			<a class="mono" href="/runs/{r.runId}" title={r.runId}>{shortId(r.runId)}</a>
		</td>
		<td>
			{MODE_NAMES[r.mode]}
			{#if r.sourceRunId}<a class="diff-link" href="/diff?b={r.runId}" title="Compare with {r.sourceRunId}">diff</a>{/if}
		</td>
		<td>
			<span class="badge {status}">{status === 'unfinished' ? 'running or crashed' : status}</span>
			{#if r.resumed > 0}<span class="badge" title="resumed after a crash or failure">resumed ×{r.resumed}</span>{/if}
		</td>
		<td class="num">{r.calls}</td>
		<td class="num">{r.live}</td>
		<td class="num">{#if r.diverged > 0}<span class="badge diverged">{r.diverged}</span>{/if}</td>
		<td>
			{#if r.diagnoses > 0}<span class="badge error" title="saved by ledger diagnose">diagnosed</span>{/if}
		</td>
		<td class="graph mono muted" title={r.graph}>{r.graph.split(':')[0].split('.').at(-1) ?? ''}</td>
		<td class="muted nowrap">{r.startedAt ? ago(ms(r.startedAt), now) : ''}</td>
		<td class="num muted">{duration === undefined ? '' : formatMs(duration)}</td>
	</tr>
{/snippet}

<div class="title">
	<h1>Runs</h1>
	{#if loaded}<span class="muted">{runs.length} in the store</span>{/if}
</div>

{#if error}
	<p class="error-box">Can't reach the trace server: {error}</p>
{/if}

{#if loaded && runs.length === 0 && !error}
	<div class="panel empty">
		<p>No runs yet.</p>
		<p class="muted">
			Record one with <code>ledger record -g module:graph --input '{'{'}…{'}'}'</code>, or call
			<code>graph.invoke(...)</code> on a graph wrapped with <code>ledger.instrument</code>.
		</p>
	</div>
{:else if runs.length > 0}
	<div class="panel table-wrap">
		<table>
			<thead>
				<tr>
					<th>Run</th>
					<th>Mode</th>
					<th>Status</th>
					<th class="num">Calls</th>
					<th class="num">Live</th>
					<th class="num">Diverged</th>
					<th>Blame</th>
					<th>Graph</th>
					<th>Started</th>
					<th class="num">Took</th>
				</tr>
			</thead>
			{#each groups as g (g.run.runId)}
				<tbody>
					{@render row(g.run, false)}
					{#each g.children as c (c.runId)}
						{@render row(c, true)}
					{/each}
				</tbody>
			{/each}
		</table>
	</div>
{/if}

<style>
	.title {
		display: flex;
		align-items: baseline;
		gap: 12px;
		margin-bottom: 16px;
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
		padding: 7px 12px;
		white-space: nowrap;
	}
	th {
		font-weight: 500;
		color: var(--muted);
		font-size: 12px;
		border-bottom: 1px solid var(--border);
	}
	tbody + tbody tr:first-child td {
		border-top: 1px solid var(--border);
	}
	tr.child td {
		padding-top: 3px;
		padding-bottom: 3px;
	}
	tr.child .id {
		padding-left: 32px;
		position: relative;
	}
	tr.child .id::before {
		content: '';
		position: absolute;
		left: 16px;
		top: 0;
		bottom: 50%;
		width: 10px;
		border-left: 1px solid var(--border);
		border-bottom: 1px solid var(--border);
	}
	tbody:hover {
		background: var(--surface-2);
	}
	.num {
		text-align: right;
		font-variant-numeric: tabular-nums;
	}
	.graph {
		max-width: 160px;
		overflow: hidden;
		text-overflow: ellipsis;
	}
	.diff-link {
		font-size: 12px;
		margin-left: 6px;
	}
	.empty {
		padding: 24px;
	}
	.empty p {
		margin: 0 0 8px;
	}
</style>

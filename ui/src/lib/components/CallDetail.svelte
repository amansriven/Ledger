<script lang="ts">
	import { EffectCall_Phase, type Event } from '#lib/gen/events_pb.js';
	import { errorOf, label, latencyMs, outcomePayload, requestPayload } from '#lib/events.js';
	import { formatMs, type Bar } from '#lib/timeline.js';
	import Payload from './Payload.svelte';

	let { bar, events, onclose }: { bar: Bar; events: Event[]; onclose: () => void } = $props();

	const KINDS = { llmCall: 'LLM call', toolCall: 'Tool call', nondet: 'Nondeterministic read', effect: 'Effect' };
	const PHASES: Record<number, string> = {
		[EffectCall_Phase.PENDING]: 'pending',
		[EffectCall_Phase.CONFIRMED]: 'confirmed',
		[EffectCall_Phase.FAILED]: 'failed'
	};
	const own = $derived(bar.events.map((i) => events[i]).filter(Boolean));
	const last = $derived(own.at(-1));
</script>

<aside class="panel">
	<div class="head">
		<div>
			<h2>{KINDS[bar.kind]} <span class="mono">{bar.label}</span></h2>
			<div class="mono muted key" title="step key">{bar.key}</div>
		</div>
		<button class="close" onclick={onclose} aria-label="Close">×</button>
	</div>

	<dl>
		<dt>Node</dt>
		<dd class="mono">{bar.node} <span class="muted">(superstep {bar.superstep})</span></dd>
		<dt>Log index</dt>
		<dd class="mono">#{bar.events.join(', #')}</dd>
		{#if last && bar.kind !== 'nondet'}
			<dt>Latency</dt>
			<dd>{formatMs(latencyMs(last))}</dd>
		{/if}
		<dt>Source</dt>
		<dd>{bar.live ? 'went live' : 'answered from the log'}</dd>
		{#if bar.diverged}
			<dt>Diverged</dt>
			<dd><span class="badge diverged">{bar.diverged}</span></dd>
		{/if}
	</dl>

	{#each own as ev (ev.index)}
		{#if ev.body.case === 'effect'}
			<div class="effect-line">
				<span class="badge {ev.body.value.phase === EffectCall_Phase.FAILED ? 'error' : ev.body.value.phase === EffectCall_Phase.PENDING ? 'unfinished' : 'ok'}">
					{PHASES[ev.body.value.phase] ?? '?'}
				</span>
				<span class="mono muted">#{ev.index}</span>
				{#if ev.body.value.attempts > 1}<span class="muted">{ev.body.value.attempts} attempts</span>{/if}
				{#if ev.body.value.reconciled}<span class="muted">settled by reconcile after a resume</span>{/if}
				<div class="mono muted key">key {ev.body.value.idempotencyKey}</div>
			</div>
		{/if}
	{/each}

	{#if last}
		{@const err = errorOf(last)}
		{#if err}
			<div class="error-box err"><strong>{err.type}</strong>: {err.message}</div>
		{/if}
		<Payload payload={requestPayload(own[0])} title={bar.kind === 'llmCall' ? 'Request' : 'Arguments'} />
		<Payload payload={outcomePayload(last)} title={bar.kind === 'nondet' ? `Value (${label(last)})` : bar.kind === 'llmCall' ? 'Response' : 'Result'} />
	{/if}
</aside>

<style>
	aside {
		padding: 16px;
		position: sticky;
		top: 16px;
		max-height: calc(100vh - 32px);
		overflow: auto;
	}
	.head {
		display: flex;
		justify-content: space-between;
		gap: 8px;
	}
	.key {
		font-size: 11.5px;
		word-break: break-all;
	}
	.close {
		font-size: 20px;
		line-height: 1;
		background: none;
		border: none;
		color: var(--muted);
		cursor: pointer;
		align-self: flex-start;
	}
	dl {
		display: grid;
		grid-template-columns: max-content 1fr;
		gap: 4px 12px;
		margin: 12px 0 0;
	}
	dt {
		color: var(--muted);
	}
	dd {
		margin: 0;
	}
	.effect-line {
		margin-top: 10px;
		display: flex;
		flex-wrap: wrap;
		gap: 4px 8px;
		align-items: baseline;
	}
	.effect-line .key {
		flex-basis: 100%;
	}
	.err {
		margin-top: 12px;
	}
</style>

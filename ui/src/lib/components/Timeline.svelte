<script lang="ts">
	import { formatMs, ticks, type Bar, type Layout } from '#lib/timeline.js';

	let {
		layout,
		selected = $bindable<Bar | undefined>(),
		highlight = []
	}: { layout: Layout; selected?: Bar; highlight?: string[] } = $props();

	// A little room past the last event, so the last bar isn't flush right.
	const span = $derived(Math.max(layout.end * 1.02, 1));
	const pct = (v: number) => (v / span) * 100;
	const axis = $derived(ticks(span));
	const KIND_NAMES = { llmCall: 'LLM', toolCall: 'tool', nondet: 'nondet', effect: 'effect' };

	function title(b: Bar): string {
		const parts = [`${KIND_NAMES[b.kind]} ${b.label}`, b.key, formatMs(b.end - b.start)];
		if (!b.live) parts.push('from the log');
		if (b.diverged) parts.push(`diverged: ${b.diverged}`);
		if (b.phase === 'pending') parts.push('in doubt: no outcome logged');
		if (b.phase === 'error' || b.phase === 'failed') parts.push(b.phase);
		return parts.join(' · ');
	}
</script>

<div class="timeline" role="group" aria-label="Run timeline">
	<div class="lane axis">
		<div class="name"></div>
		<div class="track">
			{#each axis as t (t)}
				<span class="tick" style:left="{pct(t)}%">{formatMs(t)}</span>
			{/each}
		</div>
	</div>
	{#each layout.lanes as lane (lane.id)}
		<div class="lane">
			<div class="name mono" title={lane.namespace ? `${lane.namespace} | ${lane.node}` : lane.node}>
				{#if lane.namespace}<span class="muted">{lane.namespace} |</span>{/if}
				{lane.node}
			</div>
			<div class="track">
				{#each layout.bands as band, i (band.superstep)}
					{#if i % 2 === 0}
						<div class="band" style:left="{pct(band.start)}%" style:width="{pct(band.end - band.start)}%"></div>
					{/if}
				{/each}
				{#each layout.markers as m (m.index)}
					<div class="marker {m.kind}" style:left="{pct(m.at)}%"></div>
				{/each}
				{#each lane.bars as bar (bar.events[0])}
					<button
						class="bar {bar.kind} {bar.phase}"
						class:replayed={!bar.live}
						class:diverged={bar.diverged}
						class:instant={bar.end - bar.start < 0.5}
						class:selected={selected?.key === bar.key && selected?.events[0] === bar.events[0]}
						class:highlight={highlight.includes(bar.key)}
						style:left="{pct(bar.start)}%"
						style:width="{pct(bar.end - bar.start)}%"
						title={title(bar)}
						aria-label={title(bar)}
						onclick={() => (selected = bar)}
					></button>
				{/each}
			</div>
		</div>
	{/each}
	{#if layout.markers.length}
		<div class="lane legend-row">
			<div class="name"></div>
			<div class="track">
				{#each layout.markers as m (m.index)}
					<span class="marker-label {m.kind}" style:left="{pct(m.at)}%">{m.kind}</span>
				{/each}
			</div>
		</div>
	{/if}
</div>

<div class="legend muted">
	<span><i class="swatch llmCall"></i>LLM call</span>
	<span><i class="swatch toolCall"></i>tool call</span>
	<span><i class="swatch effect"></i>effect</span>
	<span><i class="swatch nondet"></i>clock, id or random read</span>
	<span><i class="swatch toolCall replayed"></i>answered from the log</span>
	<span><i class="swatch toolCall diverged"></i>diverged</span>
</div>

<style>
	.timeline {
		--name-width: 150px;
		padding: 8px 0;
		overflow-x: auto;
	}
	.lane {
		display: flex;
		align-items: center;
		min-width: 560px;
	}
	.name {
		flex: 0 0 var(--name-width);
		padding: 0 12px;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.track {
		position: relative;
		flex: 1;
		height: 30px;
		margin-right: 16px;
	}
	.lane + .lane .track {
		border-top: 1px dashed var(--border);
	}
	.axis .track,
	.legend-row .track {
		height: 22px;
		border-top: none !important;
	}
	.tick,
	.marker-label {
		position: absolute;
		transform: translateX(-50%);
		font-size: 11px;
		color: var(--muted);
		white-space: nowrap;
		top: 2px;
	}
	.tick:first-child {
		transform: none;
	}
	.band {
		position: absolute;
		top: 0;
		bottom: 0;
		background: var(--band);
	}
	.marker {
		position: absolute;
		top: 0;
		bottom: 0;
		border-left: 1px solid var(--muted);
		opacity: 0.6;
	}
	.marker.resumed {
		border-left: 2px dashed var(--pending);
		opacity: 1;
	}
	.marker.failed,
	.marker-label.failed {
		border-color: var(--error);
		color: var(--error);
	}
	.marker-label.resumed {
		color: var(--pending);
	}
	.bar {
		position: absolute;
		top: 6px;
		height: 18px;
		min-width: 4px;
		padding: 0;
		border: 1.5px solid var(--c);
		border-radius: 4px;
		background: var(--c);
		cursor: pointer;
	}
	.bar.llmCall {
		--c: var(--llm);
	}
	.bar.toolCall {
		--c: var(--tool);
	}
	.bar.effect {
		--c: var(--effect);
	}
	.bar.nondet {
		--c: var(--nondet);
	}
	.bar.instant {
		width: 8px !important;
		margin-left: -4px;
		top: 10px;
		height: 10px;
		border-radius: 50%;
	}
	.bar.replayed {
		background: color-mix(in srgb, var(--c) 18%, var(--surface));
	}
	.bar.diverged {
		box-shadow: 0 0 0 2px var(--diverged);
	}
	.bar.error,
	.bar.failed {
		border-color: var(--error);
		background: repeating-linear-gradient(
			-45deg,
			var(--c),
			var(--c) 4px,
			var(--error) 4px,
			var(--error) 6px
		);
	}
	.bar.pending {
		border-style: dashed;
		background: color-mix(in srgb, var(--c) 25%, var(--surface));
	}
	.bar.selected {
		outline: 2px solid var(--text);
		outline-offset: 2px;
		z-index: 2;
	}
	.bar.highlight {
		box-shadow:
			0 0 0 2px var(--surface),
			0 0 0 4px var(--error);
		z-index: 1;
	}
	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 6px 16px;
		padding: 8px 12px 12px;
		font-size: 12px;
		border-top: 1px solid var(--border);
	}
	.swatch {
		display: inline-block;
		width: 14px;
		height: 10px;
		border-radius: 3px;
		margin-right: 6px;
		vertical-align: -1px;
		border: 1.5px solid var(--c);
		background: var(--c);
	}
	.swatch.llmCall {
		--c: var(--llm);
	}
	.swatch.toolCall {
		--c: var(--tool);
	}
	.swatch.effect {
		--c: var(--effect);
	}
	.swatch.nondet {
		--c: var(--nondet);
	}
	.swatch.replayed {
		background: color-mix(in srgb, var(--c) 18%, var(--surface));
	}
	.swatch.diverged {
		box-shadow: 0 0 0 2px var(--diverged);
	}
</style>

// Laying a run's log out on a timeline: one lane per node, one bar per call.
//
// A call's event is written when the call returns, so its bar ends at
// recorded_at and starts latency earlier. An effect is one bar from its
// PENDING record (written before the call) to the CONFIRMED or FAILED one;
// left PENDING, it is in doubt. Nondeterministic reads are instants.
// Times are milliseconds from the run's first event.
import { EffectCall_Phase, type Event } from './gen/events_pb';
import { DIVERGENCE_NAMES, latencyMs, label, ms, stepKey, type CallKind } from './events';

export type Phase = 'done' | 'error' | 'pending' | 'failed';

export interface Bar {
	key: string;
	kind: CallKind;
	node: string;
	namespace: string;
	superstep: number;
	label: string;
	start: number;
	end: number;
	/** Went live (false: answered from the log on a replay or fork). */
	live: boolean;
	phase: Phase;
	/** Why a replay or fork stopped following the recording here. */
	diverged?: string;
	/** Indexes of this bar's events in the log (an effect has two). */
	events: number[];
}

export interface Lane {
	id: string;
	node: string;
	namespace: string;
	bars: Bar[];
}

export interface Band {
	superstep: number;
	start: number;
	end: number;
}

export interface Marker {
	at: number;
	kind: 'resumed' | 'finished' | 'failed';
	index: number;
}

export interface Layout {
	/** recorded_at of the first event, in ms since the epoch. */
	t0: number;
	/** The latest time on the timeline. */
	end: number;
	lanes: Lane[];
	bands: Band[];
	markers: Marker[];
}

export function layout(events: Event[]): Layout {
	const t0 = events.length ? ms(events[0].recordedAt) : 0;
	const at = (ev: Event) => ms(ev.recordedAt) - t0;
	const lanes = new Map<string, Lane>();
	const bars: Bar[] = [];
	const effects = new Map<string, Bar>();
	const diverged = new Map<string, string>();
	const markers: Marker[] = [];
	let end = 0;

	for (const ev of events) {
		const t = at(ev);
		end = Math.max(end, t);
		const index = Number(ev.index);
		switch (ev.body.case) {
			case 'diverged':
				diverged.set(stepKey(ev.step), DIVERGENCE_NAMES[ev.body.value.reason]);
				continue;
			case 'runResumed':
				markers.push({ at: t, kind: 'resumed', index });
				continue;
			case 'runFinished':
				markers.push({ at: t, kind: ev.body.value.error ? 'failed' : 'finished', index });
				continue;
			case 'llmCall':
			case 'toolCall':
			case 'nondet':
			case 'effect':
				break;
			default:
				continue;
		}
		const key = stepKey(ev.step);
		const kind = ev.body.case;
		if (kind === 'effect') {
			const phase = ev.body.value.phase;
			const open = effects.get(key);
			if (open && open.phase === 'pending') {
				open.end = Math.max(open.end, t);
				open.events.push(index);
				open.live ||= !ev.fromLog;
				if (phase !== EffectCall_Phase.PENDING)
					open.phase = phase === EffectCall_Phase.FAILED ? 'failed' : 'done';
				continue;
			}
		}
		const step = ev.step;
		const bar: Bar = {
			key,
			kind,
			node: step?.node ?? '',
			namespace: step?.namespace ?? '',
			superstep: Number(step?.superstep ?? 0),
			label: label(ev),
			start: kind === 'effect' || kind === 'nondet' ? t : Math.max(0, t - latencyMs(ev)),
			end: t,
			live: !ev.fromLog,
			phase: phaseOf(ev),
			diverged: diverged.get(key),
			events: [index]
		};
		diverged.delete(key);
		if (kind === 'effect') effects.set(key, bar);
		bars.push(bar);
		const laneId = `${bar.namespace}|${bar.node}`;
		let lane = lanes.get(laneId);
		if (!lane) {
			lane = { id: laneId, node: bar.node, namespace: bar.namespace, bars: [] };
			lanes.set(laneId, lane);
		}
		lane.bars.push(bar);
	}

	const byStep = new Map<number, Band>();
	for (const b of bars) {
		const band = byStep.get(b.superstep);
		if (!band) byStep.set(b.superstep, { superstep: b.superstep, start: b.start, end: b.end });
		else {
			band.start = Math.min(band.start, b.start);
			band.end = Math.max(band.end, b.end);
		}
	}
	return {
		t0,
		end,
		lanes: [...lanes.values()],
		bands: [...byStep.values()].sort((a, b) => a.superstep - b.superstep),
		markers
	};
}

function phaseOf(ev: Event): Phase {
	if (ev.body.case === 'effect') {
		const p = ev.body.value.phase;
		if (p === EffectCall_Phase.PENDING) return 'pending';
		if (p === EffectCall_Phase.FAILED) return 'failed';
		return 'done';
	}
	if ((ev.body.case === 'llmCall' || ev.body.case === 'toolCall') && ev.body.value.error)
		return 'error';
	return 'done';
}

/** Round, evenly spaced tick values (ms) covering 0..end. */
export function ticks(end: number, target = 6): number[] {
	if (end <= 0) return [0];
	const raw = end / target;
	const pow = 10 ** Math.floor(Math.log10(raw));
	const step = [1, 2, 5, 10].map((m) => m * pow).find((s) => s >= raw) ?? 10 * pow;
	const out: number[] = [];
	for (let v = 0; v <= end + 1e-9; v += step) out.push(Math.round(v * 1000) / 1000);
	return out;
}

export function formatMs(v: number): string {
	if (v >= 60_000) return `${(v / 60_000).toFixed(1)} min`;
	if (v >= 1000) return `${(v / 1000).toFixed(v >= 10_000 ? 0 : 1)} s`;
	return `${Math.round(v)} ms`;
}

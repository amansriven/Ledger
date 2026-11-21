// Diffing two runs call by call. Calls are matched by step key (where in the
// graph they happened), the way replay matches them, so parallel branches
// that finished in another order still line up. Requests are compared by
// their normalized match key (what replay compares), outcomes by the digest
// of their canonical JSON, so neither needs the payload bytes.
import { EffectCall_Phase, type Event, type Payload } from './gen/events_pb';
import {
	DIVERGENCE_NAMES,
	errorOf,
	isCall,
	label,
	outcomePayload,
	requestPayload,
	stepKey,
	type CallKind
} from './events';

export type RowStatus = 'same' | 'changed' | 'edited' | 'rerun' | 'diverged' | 'onlyA' | 'onlyB';

export interface DiffRow {
	key: string;
	kind: CallKind;
	node: string;
	superstep: number;
	label: string;
	/** The call in each run; an effect's settled event (or PENDING if none). */
	a?: Event;
	b?: Event;
	status: RowStatus;
	/** For `diverged`: why B stopped following its recording here. */
	reason?: string;
	requestChanged: boolean;
	outcomeChanged: boolean;
}

export interface RunDiff {
	rows: DiffRow[];
	counts: Record<RowStatus, number>;
	/** Whether the runs ended the same way (same output or same error). */
	sameEnding: boolean;
}

/** Each step's call, in log order; an effect's last event stands for it. */
function callsByKey(events: Event[]): Map<string, Event> {
	const out = new Map<string, Event>();
	for (const ev of events) {
		if (!isCall(ev)) continue;
		const key = stepKey(ev.step);
		const prev = out.get(key);
		if (prev && ev.body.case === 'effect' && prev.body.case === 'effect') {
			// Keep the effect at its first position, with its latest outcome.
			out.delete(key);
		}
		out.set(key, ev);
	}
	return out;
}

function requestKey(ev: Event): string {
	switch (ev.body.case) {
		case 'llmCall':
			return ev.body.value.requestKey || (ev.body.value.request?.sha256 ?? '');
		case 'toolCall':
			return ev.body.value.argsKey || (ev.body.value.args?.sha256 ?? '');
		case 'effect':
			return ev.body.value.argsKey || (ev.body.value.args?.sha256 ?? '');
		case 'nondet':
			return ev.body.value.source;
		default:
			return '';
	}
}

function outcomeKey(ev: Event): string {
	const err = errorOf(ev);
	if (err) return `!${err.type}:${err.message}`;
	if (ev.body.case === 'effect' && ev.body.value.phase === EffectCall_Phase.PENDING) return '?pending';
	return outcomePayload(ev)?.sha256 ?? '';
}

function divergences(events: Event[]): Map<string, string> {
	const out = new Map<string, string>();
	for (const ev of events)
		if (ev.body.case === 'diverged') out.set(stepKey(ev.step), DIVERGENCE_NAMES[ev.body.value.reason]);
	return out;
}

function ending(events: Event[]): string {
	const fin = events.findLast((e) => e.body.case === 'runFinished');
	if (!fin || fin.body.case !== 'runFinished') return 'unfinished';
	const err = fin.body.value.error;
	return err ? `!${err.type}:${err.message}` : (fin.body.value.output?.sha256 ?? '');
}

export function diffRuns(a: Event[], b: Event[]): RunDiff {
	const ca = callsByKey(a);
	const cb = callsByKey(b);
	const div = divergences(b);
	// Order: B's log order. A call only A made goes right after the last call
	// before it (in A's log) that B made too, or first if there is none.
	const anchored = new Map<string, string[]>();
	let anchor = '';
	for (const key of ca.keys()) {
		if (cb.has(key)) anchor = key;
		else anchored.set(anchor, [...(anchored.get(anchor) ?? []), key]);
	}
	const keys = [...(anchored.get('') ?? [])];
	for (const key of cb.keys()) keys.push(key, ...(anchored.get(key) ?? []));

	const rows: DiffRow[] = [];
	for (const key of keys) {
		const ea = ca.get(key);
		const eb = cb.get(key);
		const ev = (eb ?? ea)!;
		const step = ev.step;
		const row: DiffRow = {
			key,
			kind: ev.body.case as CallKind,
			node: step?.node ?? '',
			superstep: Number(step?.superstep ?? 0),
			label: label(ev),
			a: ea,
			b: eb,
			status: 'same',
			requestChanged: false,
			outcomeChanged: false
		};
		if (!ea) row.status = 'onlyB';
		else if (!eb) row.status = 'onlyA';
		else {
			row.requestChanged = requestKey(ea) !== requestKey(eb);
			row.outcomeChanged = outcomeKey(ea) !== outcomeKey(eb);
			const reason = div.get(key);
			if (reason === 'edited' || reason === 'rerun') row.status = reason;
			else if (reason) {
				row.status = 'diverged';
				row.reason = reason;
			} else if (row.requestChanged || row.outcomeChanged) row.status = 'changed';
		}
		rows.push(row);
	}
	const counts: Record<RowStatus, number> = {
		same: 0, changed: 0, edited: 0, rerun: 0, diverged: 0, onlyA: 0, onlyB: 0
	};
	for (const r of rows) counts[r.status]++;
	return { rows, counts, sameEnding: ending(a) === ending(b) };
}

export function payloadsOf(row: DiffRow): { request: [Payload?, Payload?]; outcome: [Payload?, Payload?] } {
	return {
		request: [row.a && requestPayload(row.a), row.b && requestPayload(row.b)],
		outcome: [row.a && outcomePayload(row.a), row.b && outcomePayload(row.b)]
	};
}

// Reading ledger.v1.Event values: kinds, step keys, payloads, summaries.
// Mirrors the Python side (ledger/core/store.py step_key, ledger/replay/fork.py
// _summary and _label) so the UI names steps the way `ledger steps` does.
import { timestampMs, type Timestamp } from '@bufbuild/protobuf/wkt';
import {
	DivergenceReason,
	EffectCall_Phase,
	Mode,
	type Error as LedgerError,
	type Event,
	type Payload,
	type StepRef
} from './gen/events_pb';
import { RunStatus } from './gen/trace_service_pb';

export type Kind = NonNullable<Event['body']['case']>;
export type CallKind = 'llmCall' | 'toolCall' | 'nondet' | 'effect';

export const CALL_KINDS: readonly CallKind[] = ['llmCall', 'toolCall', 'nondet', 'effect'];

export function kindOf(ev: Event): Kind | undefined {
	return ev.body.case;
}

export function isCall(ev: Event): boolean {
	const k = ev.body.case;
	return k === 'llmCall' || k === 'toolCall' || k === 'nondet' || k === 'effect';
}

/** A call as `ledger runs` counts them (ledger/cli.py _is_call): LLM and
 * tool calls and settled effects, not an effect's PENDING record or a
 * clock, id or random read. */
export function countsAsCall(ev: Event): boolean {
	const k = ev.body.case;
	if (k === 'effect') return ev.body.value.phase !== EffectCall_Phase.PENDING;
	return k === 'llmCall' || k === 'toolCall';
}

/** Same string as ledger.core.store.step_key. */
export function stepKey(step: StepRef | undefined): string {
	if (!step) return '';
	return `${step.namespace}/${step.superstep}/${step.node}/${step.path}#${step.seq}`;
}

export function ms(ts: Timestamp | undefined): number {
	return ts ? timestampMs(ts) : 0;
}

const decoder = new TextDecoder();

/** A payload's JSON value, or `missing` when it was left out (over the
 * inline limit) and has to be fetched with GetPayload. */
export type Decoded = { state: 'none' } | { state: 'missing'; sha256: string } | { state: 'ok'; value: unknown };

export function decode(p: Payload | undefined): Decoded {
	if (!p) return { state: 'none' };
	if (p.json.length === 0) return p.sha256 ? { state: 'missing', sha256: p.sha256 } : { state: 'none' };
	return { state: 'ok', value: JSON.parse(decoder.decode(p.json)) };
}

export function decodeBytes(bytes: Uint8Array): unknown {
	return JSON.parse(decoder.decode(bytes));
}

/** Model, tool or effect name, or nondet source. */
export function label(ev: Event): string {
	switch (ev.body.case) {
		case 'llmCall':
			return ev.body.value.model;
		case 'toolCall':
			return ev.body.value.name;
		case 'nondet':
			return ev.body.value.source;
		case 'effect':
			return ev.body.value.name;
		default:
			return '';
	}
}

export function errorOf(ev: Event): LedgerError | undefined {
	switch (ev.body.case) {
		case 'llmCall':
		case 'toolCall':
		case 'effect':
			return ev.body.value.error;
		case 'runFinished':
			return ev.body.value.error;
		default:
			return undefined;
	}
}

/** The call's outcome payload: response, result or value. */
export function outcomePayload(ev: Event): Payload | undefined {
	switch (ev.body.case) {
		case 'llmCall':
			return ev.body.value.response;
		case 'toolCall':
			return ev.body.value.result;
		case 'nondet':
			return ev.body.value.value;
		case 'effect':
			return ev.body.value.result;
		case 'runFinished':
			return ev.body.value.output;
		default:
			return undefined;
	}
}

/** The call's request payload: request, args or input. */
export function requestPayload(ev: Event): Payload | undefined {
	switch (ev.body.case) {
		case 'llmCall':
			return ev.body.value.request;
		case 'toolCall':
		case 'effect':
			return ev.body.value.args;
		case 'runStarted':
			return ev.body.value.input;
		default:
			return undefined;
	}
}

/** The outcome in one short line, like `ledger steps` prints it. */
export function summary(ev: Event, width = 80): string {
	let text: string;
	const err = errorOf(ev);
	if (ev.body.case === 'effect' && ev.body.value.phase === EffectCall_Phase.PENDING) {
		text = 'in doubt' + (err ? ` after ${err.type}` : '');
	} else if (err) {
		text = `!${err.type}: ${err.message}`;
	} else {
		const d = decode(outcomePayload(ev));
		if (d.state === 'missing') return '(large payload)';
		if (d.state === 'none') return '';
		let v = d.value;
		if (ev.body.case === 'llmCall' && v && typeof v === 'object' && 'choices' in v) {
			const choices = (v as { choices?: { message?: { content?: string } }[] }).choices;
			v = choices?.[0]?.message?.content ?? '';
		}
		text = typeof v === 'string' ? v : JSON.stringify(v);
	}
	text = text.split(/\s+/).join(' ').trim();
	return text.length <= width ? text : text.slice(0, width - 1) + '…';
}

export function latencyMs(ev: Event): number {
	switch (ev.body.case) {
		case 'llmCall':
		case 'toolCall':
		case 'effect':
			return Number(ev.body.value.latencyUs) / 1000;
		default:
			return 0;
	}
}

export const MODE_NAMES: Record<Mode, string> = {
	[Mode.UNSPECIFIED]: '?',
	[Mode.RECORD]: 'record',
	[Mode.REPLAY]: 'replay',
	[Mode.FORK]: 'fork'
};

export const STATUS_NAMES: Record<RunStatus, string> = {
	[RunStatus.UNSPECIFIED]: '?',
	[RunStatus.OK]: 'ok',
	[RunStatus.ERROR]: 'error',
	[RunStatus.UNFINISHED]: 'unfinished'
};

export const DIVERGENCE_NAMES: Record<DivergenceReason, string> = {
	[DivergenceReason.UNSPECIFIED]: 'diverged',
	[DivergenceReason.CHANGED_REQUEST]: 'changed request',
	[DivergenceReason.UNRECORDED_CALL]: 'unrecorded call',
	[DivergenceReason.CHANGED_KIND]: 'changed kind',
	[DivergenceReason.EDITED]: 'edited',
	[DivergenceReason.RERUN]: 'rerun'
};

/** Run status from its log, as ledger.replay.resume.run_status decides it:
 * only the last RunFinished after the last RunResumed counts. */
export function statusOf(events: Event[]): RunStatus {
	let status = RunStatus.UNFINISHED;
	for (const ev of events) {
		if (ev.body.case === 'runResumed') status = RunStatus.UNFINISHED;
		else if (ev.body.case === 'runFinished')
			status = ev.body.value.error ? RunStatus.ERROR : RunStatus.OK;
	}
	return status;
}

export function shortId(runId: string): string {
	const [base, ...rest] = runId.split('.');
	return [base.slice(0, 8), ...rest].join('.');
}

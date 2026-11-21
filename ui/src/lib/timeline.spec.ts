import { create, type MessageInitShape } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { describe, expect, it } from 'vitest';
import {
	DivergenceReason,
	EffectCall_Phase,
	EventSchema,
	type Event
} from './gen/events_pb';
import { formatMs, layout, ticks } from './timeline';

const T0 = 1_700_000_000_000;
let index = 0n;

type Init = MessageInitShape<typeof EventSchema>;

function ev(at: number, init: Pick<Init, 'step' | 'fromLog' | 'body'>): Event {
	return create(EventSchema, { ...init, index: index++, recordedAt: timestampFromMs(T0 + at) } as Init);
}

function step(node: string, superstep: number, seq = 0) {
	return { namespace: '', superstep: BigInt(superstep), node, path: `(${node})`, seq: BigInt(seq) };
}

function run(): Event[] {
	index = 0n;
	return [
		ev(0, { body: { case: 'runStarted', value: {} } }),
		ev(5, { step: step('plan', 1), body: { case: 'nondet', value: { source: 'time' } } }),
		ev(100, { step: step('plan', 1, 1), body: { case: 'llmCall', value: { model: 'm', latencyUs: 80_000n } } }),
		ev(150, { step: step('a', 2), fromLog: true, body: { case: 'toolCall', value: { name: 'search', latencyUs: 30_000n } } }),
		ev(160, { step: step('b', 2), body: { case: 'diverged', value: { reason: DivergenceReason.RERUN } } }),
		ev(170, { step: step('b', 2), body: { case: 'toolCall', value: { name: 'search', latencyUs: 50_000n, error: { type: 'TimeoutError' } } } }),
		ev(200, { step: step('pay', 3), body: { case: 'effect', value: { name: 'refund', phase: EffectCall_Phase.PENDING } } }),
		ev(260, { step: step('pay', 3), body: { case: 'effect', value: { name: 'refund', phase: EffectCall_Phase.CONFIRMED } } }),
		ev(300, { body: { case: 'runFinished', value: {} } })
	];
}

describe('layout', () => {
	it('puts each call on its node lane, ending when it was logged', () => {
		const l = layout(run());
		expect(l.t0).toBe(T0);
		expect(l.end).toBe(300);
		expect(l.lanes.map((x) => x.node)).toEqual(['plan', 'a', 'b', 'pay']);
		const [time, llm] = l.lanes[0].bars;
		expect([time.kind, time.start, time.end]).toEqual(['nondet', 5, 5]);
		expect([llm.kind, llm.start, llm.end, llm.label]).toEqual(['llmCall', 20, 100, 'm']);
	});

	it('marks replayed calls, errors and divergences', () => {
		const l = layout(run());
		const a = l.lanes[1].bars[0];
		expect(a.live).toBe(false);
		const b = l.lanes[2].bars[0];
		expect([b.live, b.phase, b.diverged, b.start]).toEqual([true, 'error', 'rerun', 120]);
	});

	it('joins an effect from PENDING to CONFIRMED', () => {
		const [refund] = layout(run()).lanes[3].bars;
		expect([refund.start, refund.end, refund.phase, refund.events]).toEqual([200, 260, 'done', [6, 7]]);
	});

	it('leaves an effect with no outcome pending', () => {
		const events = run().slice(0, 7);
		const [refund] = layout(events).lanes[3].bars;
		expect([refund.phase, refund.events]).toEqual(['pending', [6]]);
	});

	it('spans each superstep and marks the end of the run', () => {
		const l = layout(run());
		expect(l.bands).toEqual([
			{ superstep: 1, start: 5, end: 100 },
			{ superstep: 2, start: 120, end: 170 },
			{ superstep: 3, start: 200, end: 260 }
		]);
		expect(l.markers).toEqual([{ at: 300, kind: 'finished', index: 8 }]);
	});

	it('handles an empty log', () => {
		expect(layout([])).toEqual({ t0: 0, end: 0, lanes: [], bands: [], markers: [] });
	});
});

describe('ticks', () => {
	it('picks round steps', () => {
		expect(ticks(1000)).toEqual([0, 200, 400, 600, 800, 1000]);
		expect(ticks(0)).toEqual([0]);
		expect(ticks(37)).toEqual([0, 10, 20, 30]);
	});
	it('formats', () => {
		expect([formatMs(12.4), formatMs(1500), formatMs(12_000), formatMs(90_000)]).toEqual([
			'12 ms', '1.5 s', '12 s', '1.5 min'
		]);
	});
});

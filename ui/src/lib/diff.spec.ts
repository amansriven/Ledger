import { create, type MessageInitShape } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import { DivergenceReason, EffectCall_Phase, EventSchema, type Event } from './gen/events_pb';
import { diffRuns } from './diff';

type Init = MessageInitShape<typeof EventSchema>;
const step = (node: string, superstep = 1, seq = 0) => ({
	namespace: '', superstep: BigInt(superstep), node, path: `(${node})`, seq: BigInt(seq)
});
const sha = (s: string) => ({ sha256: s, json: new TextEncoder().encode(JSON.stringify(s)) });

function log(...bodies: Pick<Init, 'step' | 'body' | 'fromLog'>[]): Event[] {
	return [
		{ body: { case: 'runStarted', value: {} } },
		...bodies,
		{ body: { case: 'runFinished', value: { output: sha('out') } } }
	].map((b, i) => create(EventSchema, { ...b, index: BigInt(i) } as Init));
}

const llm = (node: string, req: string, resp: string, superstep = 1): Pick<Init, 'step' | 'body'> => ({
	step: step(node, superstep),
	body: { case: 'llmCall', value: { model: 'm', requestKey: req, response: sha(resp) } }
});

describe('diffRuns', () => {
	it('calls a replay identical to its recording the same', () => {
		const a = log(llm('plan', 'r1', 'x'), llm('write', 'r2', 'y', 2));
		const b = log(llm('plan', 'r1', 'x'), llm('write', 'r2', 'y', 2));
		const d = diffRuns(a, b);
		expect(d.rows.map((r) => r.status)).toEqual(['same', 'same']);
		expect(d.sameEnding).toBe(true);
	});

	it('names edits, reruns and what changed downstream', () => {
		const a = log(llm('plan', 'r1', 'x'), llm('web', 'r2', 'y', 2), llm('write', 'r3', 'z', 3));
		const b = log(
			llm('plan', 'r1', 'x'),
			{ step: step('web', 2), body: { case: 'diverged', value: { reason: DivergenceReason.EDITED } } },
			llm('web', 'r2', 'EDITED', 2),
			llm('write', 'r3-changed', 'z2', 3)
		);
		const d = diffRuns(a, b);
		expect(d.rows.map((r) => [r.node, r.status])).toEqual([
			['plan', 'same'], ['web', 'edited'], ['write', 'changed']
		]);
		const write = d.rows[2];
		expect([write.requestChanged, write.outcomeChanged]).toEqual([true, true]);
		expect(d.counts).toMatchObject({ same: 1, edited: 1, changed: 1 });
	});

	it('keeps a divergence reason', () => {
		const a = log(llm('plan', 'r1', 'x'));
		const b = log(
			{ step: step('plan'), body: { case: 'diverged', value: { reason: DivergenceReason.CHANGED_REQUEST } } },
			llm('plan', 'r1b', 'x')
		);
		expect(diffRuns(a, b).rows[0]).toMatchObject({ status: 'diverged', reason: 'changed request' });
	});

	it('places calls only one run made where they happened, removals first', () => {
		const a = log(llm('plan', 'r1', 'x'), llm('old', 'r2', 'y', 2), llm('write', 'r3', 'z', 3));
		const b = log(llm('plan', 'r1', 'x'), llm('new', 'r4', 'w', 2), llm('write', 'r3', 'z', 3));
		expect(diffRuns(a, b).rows.map((r) => [r.node, r.status])).toEqual([
			['plan', 'same'], ['old', 'onlyA'], ['new', 'onlyB'], ['write', 'same']
		]);
	});

	it('compares an effect by its settled outcome', () => {
		const eff = (phase: EffectCall_Phase, result?: string): Pick<Init, 'step' | 'body'> => ({
			step: step('pay'),
			body: { case: 'effect', value: { name: 'refund', argsKey: 'k', phase, result: result ? sha(result) : undefined } }
		});
		const a = log(eff(EffectCall_Phase.PENDING), eff(EffectCall_Phase.CONFIRMED, 're_1'));
		const b = log(eff(EffectCall_Phase.PENDING), eff(EffectCall_Phase.CONFIRMED, 're_1'));
		const pendingOnly = log(eff(EffectCall_Phase.PENDING));
		expect(diffRuns(a, b).rows).toHaveLength(1);
		expect(diffRuns(a, b).rows[0].status).toBe('same');
		expect(diffRuns(a, pendingOnly).rows[0]).toMatchObject({ status: 'changed', outcomeChanged: true });
	});

	it('notices a different ending', () => {
		const a = log(llm('plan', 'r1', 'x'));
		const b = log(llm('plan', 'r1', 'x')).slice(0, -1);
		expect(diffRuns(a, b).sameEnding).toBe(false);
	});
});

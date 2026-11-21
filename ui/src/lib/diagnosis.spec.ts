import { create, type MessageInitShape } from '@bufbuild/protobuf';
import { describe as group, expect, it } from 'vitest';
import { EventSchema, type Event } from './gen/events_pb';
import { candidates, describe, parseDiagnosis, passText, reranCells, type Counterfactual, type Diagnosis } from './diagnosis';

type Init = MessageInitShape<typeof EventSchema>;
const key = (node: string, superstep: number, seq = 0) => `/${superstep}/${node}/(${node})#${seq}`;
const step = (node: string, superstep: number, seq = 0) => ({
	namespace: '', superstep: BigInt(superstep), node, path: `(${node})`, seq: BigInt(seq)
});

function events(): Event[] {
	const bodies: Init[] = [
		{ body: { case: 'runStarted', value: {} } },
		{ step: step('plan', 1), body: { case: 'nondet', value: { source: 'rng_seed' } } },
		{ step: step('plan', 1, 1), body: { case: 'llmCall', value: { model: 'm' } } },
		{ step: step('web', 2), body: { case: 'toolCall', value: { name: 'search' } } },
		{ step: step('pay', 2), body: { case: 'effect', value: { name: 'refund' } } },
		{ step: step('write', 3), body: { case: 'llmCall', value: { model: 'm' } } },
		{ body: { case: 'runFinished', value: {} } }
	];
	return bodies.map((b, i) => create(EventSchema, { ...b, index: BigInt(i) } as Init));
}

const cf = (label: string, reran: string[], fixed: boolean, passed = fixed ? 1 : 0, trials = 1): Counterfactual => ({
	label, reran, fixed, passed, trials, repeats: trials, threshold: 0.5, rate: passed / trials,
	interval: [0.2, 0.9], run_ids: ['r.fork-1'], live_calls: 1, tokens_in: 0, tokens_out: 0, dollars: 0
});

function diagnosis(over: Partial<Diagnosis> = {}): Diagnosis {
	const guilty = { index: 3, key: key('web', 2), kind: 'tool_call' as const, node: 'web', superstep: 2, seq: 0, label: 'search', summary: 'BAD', from_log: false };
	return {
		schema: 'ledger.diagnosis/1', run_id: 'r', status: 'found', complete: true, step: guilty, steps: [guilty],
		narrowed: [], candidates: 4, counterfactuals: [], spent: { forks: 2, live_calls: 3, tokens_in: 0, tokens_out: 0, dollars: 0 },
		settings: { kinds: ['llm_call', 'tool_call', 'nondet'], repeats: 1, threshold: 0.5, budget: null, pricing: null },
		...over
	};
}

group('candidates', () => {
	it('lists the blamable calls in log order, marking the guilty one', () => {
		const cells = candidates(events(), diagnosis());
		expect(cells.map((c) => [c.index, c.kind, c.guilty])).toEqual([
			[1, 'nondet', false], [2, 'llmCall', false], [3, 'toolCall', true], [5, 'llmCall', false]
		]);
	});
	it('follows the kinds the diagnosis searched', () => {
		const d = diagnosis({ settings: { kinds: ['llm_call'], repeats: 1, threshold: 0.5, budget: null, pricing: null } });
		expect(candidates(events(), d).map((c) => c.index)).toEqual([2, 5]);
	});
	it('marks the narrowed range when the budget ran out', () => {
		const n = diagnosis().steps[0];
		const d = diagnosis({ status: 'budget', steps: [], narrowed: [n, { ...n, index: 5, key: key('write', 3) }] });
		expect(candidates(events(), d).map((c) => c.narrowed)).toEqual([false, false, true, true]);
	});
});

group('counterfactuals', () => {
	const cells = candidates(events(), diagnosis());
	it('says which calls each one reran', () => {
		const from = cf(`from ${key('web', 2)}`, [key('web', 2), key('write', 3)], true);
		expect(reranCells(cells, from)).toEqual([false, false, true, true]);
		expect(describe(from, cells)).toBe('rerun from #3 on');
		const forced = cf(`from ${key('write', 3)}`, [key('plan', 1, 1), key('write', 3)], false);
		expect(describe(forced, cells)).toBe('rerun from #5 on, with #2');
		expect(describe(cf(`only ${key('web', 2)}`, [key('web', 2)], true), cells)).toBe('rerun #3 alone');
	});
	it('reports pass rates', () => {
		expect(passText(cf('x', [], true))).toBe('1 of 1 passed');
		expect(passText(cf('x', [], true, 3, 5))).toBe('3 of 5 passed (60%, 20–90%)');
	});
	it('rejects an unknown schema', () => {
		expect(() => parseDiagnosis('{"schema": "other/9"}')).toThrow(/unknown diagnosis schema/);
		expect(parseDiagnosis(JSON.stringify(diagnosis())).status).toBe('found');
	});
});

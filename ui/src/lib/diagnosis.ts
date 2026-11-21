// Saved diagnoses (`ledger diagnose`, schema ledger.diagnosis/1): their JSON
// shape, and the candidate strip the blame view draws from them.
import type { Event } from './gen/events_pb';
import { label, stepKey, type CallKind } from './events';

export const SCHEMA = 'ledger.diagnosis/1';

/** A recorded call, as ledger.replay.fork.StepInfo serializes it. */
export interface StepInfo {
	index: number;
	key: string;
	kind: 'llm_call' | 'tool_call' | 'nondet' | 'effect';
	node: string;
	superstep: number;
	seq: number;
	label: string;
	summary: string;
	from_log: boolean;
}

export interface Counterfactual {
	/** "from <key>" (that call and every later one) or "only <keys>". */
	label: string;
	/** Keys of the calls rerun live; what depends on them reran too. */
	reran: string[];
	fixed: boolean;
	passed: number;
	trials: number;
	repeats: number;
	threshold: number;
	rate: number;
	interval: [number, number];
	run_ids: string[];
	live_calls: number;
	tokens_in: number;
	tokens_out: number;
	dollars: number;
}

export type Status = 'found' | 'no_step' | 'budget' | 'inconsistent';

export interface Diagnosis {
	schema: string;
	run_id: string;
	status: Status;
	complete: boolean;
	step: StepInfo | null;
	steps: StepInfo[];
	narrowed: StepInfo[];
	candidates: number;
	counterfactuals: Counterfactual[];
	spent: { forks: number; live_calls: number; tokens_in: number; tokens_out: number; dollars: number };
	graph?: string;
	check?: string;
	settings?: {
		kinds: string[];
		repeats: number;
		threshold: number;
		budget: { forks: number | null; live_calls: number | null; dollars: number | null } | null;
		pricing: { input_per_mtok: number; output_per_mtok: number } | null;
	};
}

export function parseDiagnosis(json: string): Diagnosis {
	const d = JSON.parse(json) as Diagnosis;
	if (d.schema !== SCHEMA) throw new Error(`unknown diagnosis schema ${JSON.stringify(d.schema)}`);
	return d;
}

const KIND: Record<string, CallKind> = {
	llm_call: 'llmCall',
	tool_call: 'toolCall',
	nondet: 'nondet',
	effect: 'effect'
};

export interface Cell {
	/** The call's index in the failed run's log. */
	index: number;
	key: string;
	kind: CallKind;
	node: string;
	label: string;
	guilty: boolean;
	/** With status "budget": among the calls the search was narrowed to. */
	narrowed: boolean;
}

/** The calls the diagnosis could blame, in log order. Effects are never
 * candidates (rerunning one would repeat it), and neither is an effect's
 * PENDING record. */
export function candidates(events: Event[], d: Diagnosis): Cell[] {
	const kinds = new Set((d.settings?.kinds ?? ['llm_call', 'tool_call', 'nondet']).map((k) => KIND[k]));
	const guilty = new Set(d.steps.map((s) => s.key));
	const narrowed = new Set(d.narrowed.map((s) => s.key));
	const cells: Cell[] = [];
	for (const ev of events) {
		const kind = ev.body.case;
		if (kind !== 'llmCall' && kind !== 'toolCall' && kind !== 'nondet') continue;
		if (!kinds.has(kind)) continue;
		const key = stepKey(ev.step);
		cells.push({
			index: Number(ev.index),
			key,
			kind,
			node: ev.step?.node ?? '',
			label: label(ev),
			guilty: guilty.has(key),
			narrowed: narrowed.has(key)
		});
	}
	return cells;
}

/** Which cells a counterfactual reran. */
export function reranCells(cells: Cell[], cf: Counterfactual): boolean[] {
	const reran = new Set(cf.reran);
	return cells.map((c) => reran.has(c.key));
}

/** "from #12" or "#7 alone", naming calls by their log index. */
export function describe(cf: Counterfactual, cells: Cell[]): string {
	const byKey = new Map(cells.map((c) => [c.key, c.index]));
	const name = (key: string) => (byKey.has(key) ? `#${byKey.get(key)}` : key);
	if (cf.label.startsWith('from ')) {
		const from = cf.label.slice(5);
		const forced = cf.reran.filter((k) => (byKey.get(k) ?? 0) < (byKey.get(from) ?? 0));
		const base = `rerun from ${name(from)} on`;
		return forced.length ? `${base}, with ${forced.map(name).join(', ')}` : base;
	}
	return `rerun ${cf.reran.map(name).join(', ')} alone`;
}

const pct = (v: number) => `${Math.round(v * 100)}%`;

/** "1 of 1 passed" or "3 of 5 passed (60%, 23–88%)". */
export function passText(cf: Counterfactual): string {
	const base = `${cf.passed} of ${cf.trials} passed`;
	if (cf.trials <= 1) return base;
	const [lo, hi] = cf.interval.map((v) => Math.round(v * 100));
	return `${base} (${pct(cf.rate)}, ${lo}–${hi}%)`;
}

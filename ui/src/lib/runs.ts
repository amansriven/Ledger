// Grouping runs for the runs list: each recording with the replays and forks
// made from it beneath it, newest recording first.
import type { RunSummary } from './gen/trace_service_pb';
import { ms } from './events';

export interface RunGroup {
	run: RunSummary;
	/** Replays and forks of `run`, oldest first. */
	children: RunSummary[];
}

export function groupRuns(runs: RunSummary[]): RunGroup[] {
	const ids = new Set(runs.map((r) => r.runId));
	const children = new Map<string, RunSummary[]>();
	const roots: RunSummary[] = [];
	for (const r of runs) {
		// A replay or fork whose source isn't in the store stands on its own.
		if (r.sourceRunId && ids.has(r.sourceRunId)) {
			const list = children.get(r.sourceRunId) ?? [];
			list.push(r);
			children.set(r.sourceRunId, list);
		} else roots.push(r);
	}
	const started = (r: RunSummary) => ms(r.startedAt);
	// Replays of replays hang under the recording they eventually come from.
	const flatten = (id: string): RunSummary[] =>
		(children.get(id) ?? []).flatMap((c) => [c, ...flatten(c.runId)]);
	return roots
		.sort((a, b) => started(b) - started(a))
		.map((run) => ({ run, children: flatten(run.runId).sort((a, b) => started(a) - started(b)) }));
}

export function durationMs(r: RunSummary): number | undefined {
	if (!r.startedAt || !r.finishedAt) return undefined;
	return ms(r.finishedAt) - ms(r.startedAt);
}

export function ago(at: number, now = Date.now()): string {
	const s = Math.max(0, Math.round((now - at) / 1000));
	if (s < 60) return `${s}s ago`;
	const m = Math.round(s / 60);
	if (m < 60) return `${m}m ago`;
	const h = Math.round(m / 60);
	if (h < 48) return `${h}h ago`;
	return new Date(at).toLocaleDateString();
}

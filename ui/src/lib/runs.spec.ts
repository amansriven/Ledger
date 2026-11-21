import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { describe, expect, it } from 'vitest';
import { RunSummarySchema } from './gen/trace_service_pb';
import { ago, durationMs, groupRuns } from './runs';

const run = (runId: string, at: number, sourceRunId = '', finished?: number) =>
	create(RunSummarySchema, {
		runId,
		sourceRunId,
		startedAt: timestampFromMs(at),
		finishedAt: finished === undefined ? undefined : timestampFromMs(finished)
	});

describe('groupRuns', () => {
	it('nests replays and forks under their recording, newest recording first', () => {
		const groups = groupRuns([
			run('a', 1),
			run('b', 2),
			run('a.fork-1', 5, 'a'),
			run('a.replay-1', 3, 'a'),
			run('a.fork-1.replay-1', 6, 'a.fork-1'),
			run('x.fork-1', 4, 'x') // source missing from the store
		]);
		expect(groups.map((g) => g.run.runId)).toEqual(['x.fork-1', 'b', 'a']);
		expect(groups[2].children.map((c) => c.runId)).toEqual(['a.replay-1', 'a.fork-1', 'a.fork-1.replay-1']);
	});
});

describe('durationMs and ago', () => {
	it('measures finished runs only', () => {
		expect(durationMs(run('a', 1000, '', 1750))).toBe(750);
		expect(durationMs(run('a', 1000))).toBeUndefined();
	});
	it('says how long ago', () => {
		expect([ago(0, 5_000), ago(0, 300_000), ago(0, 7_200_000)]).toEqual(['5s ago', '5m ago', '2h ago']);
	});
});

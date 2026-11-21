import { describe, expect, it } from 'vitest';
import { asText, lineDiff } from './textdiff';

const ops = (a: string, b: string) => lineDiff(a, b).map((o) => `${{ same: ' ', del: '-', add: '+' }[o.op]}${o.text}`);

describe('lineDiff', () => {
	it('marks changed lines and keeps the rest', () => {
		expect(ops('a\nb\nc\nd', 'a\nB\nc\nd\ne')).toEqual([' a', '-b', '+B', ' c', ' d', '+e']);
	});
	it('handles equal and empty inputs', () => {
		expect(ops('x\ny', 'x\ny')).toEqual([' x', ' y']);
		expect(ops('', 'x')).toEqual(['-', '+x']);
	});
	it('finds the common lines between edits', () => {
		expect(ops('1\n2\n3\n4\n5', '1\n3\nX\n5')).toEqual([' 1', '-2', ' 3', '-4', '+X', ' 5']);
	});
});

describe('asText', () => {
	it('sorts keys so key order is not a change', () => {
		expect(asText({ b: 1, a: { d: 2, c: [{ f: 1, e: 2 }] } })).toBe(asText({ a: { c: [{ e: 2, f: 1 }], d: 2 }, b: 1 }));
		expect(asText('plain')).toBe('plain');
	});
});

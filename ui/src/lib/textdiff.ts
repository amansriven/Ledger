// A line diff for showing two payloads against each other: the longest
// common subsequence of lines, as unified-diff operations.

export type Op = { op: 'same' | 'del' | 'add'; text: string };

/** Above this many line pairs the diff gives up on alignment and shows A
 * removed and B added, rather than spend seconds on a huge payload. */
const MAX_CELLS = 4_000_000;

export function lineDiff(a: string, b: string): Op[] {
	const x = a.split('\n');
	const y = b.split('\n');
	// Trim the common head and tail first: payloads usually differ in a few lines.
	let head = 0;
	while (head < x.length && head < y.length && x[head] === y[head]) head++;
	let tail = 0;
	while (
		tail < x.length - head &&
		tail < y.length - head &&
		x[x.length - 1 - tail] === y[y.length - 1 - tail]
	)
		tail++;
	const xs = x.slice(head, x.length - tail);
	const ys = y.slice(head, y.length - tail);
	const same = (lines: string[]): Op[] => lines.map((text) => ({ op: 'same', text }));
	const out: Op[] = same(x.slice(0, head));

	if (xs.length * ys.length > MAX_CELLS) {
		out.push(...xs.map((text): Op => ({ op: 'del', text })), ...ys.map((text): Op => ({ op: 'add', text })));
	} else {
		// lcs[i][j]: LCS length of xs[i..] and ys[j..].
		const n = xs.length;
		const m = ys.length;
		const lcs = Array.from({ length: n + 1 }, () => new Uint32Array(m + 1));
		for (let i = n - 1; i >= 0; i--)
			for (let j = m - 1; j >= 0; j--)
				lcs[i][j] = xs[i] === ys[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
		let i = 0;
		let j = 0;
		while (i < n || j < m) {
			if (i < n && j < m && xs[i] === ys[j]) {
				out.push({ op: 'same', text: xs[i] });
				i++;
				j++;
			} else if (j < m && (i === n || lcs[i][j + 1] > lcs[i + 1][j])) {
				out.push({ op: 'add', text: ys[j++] });
			} else {
				out.push({ op: 'del', text: xs[i++] });
			}
		}
	}
	out.push(...same(x.slice(x.length - tail)));
	return out;
}

/** Show a value as text for diffing: strings as they are, the rest as
 * indented JSON with sorted keys, so key order never shows as a change. */
export function asText(v: unknown): string {
	if (typeof v === 'string') return v;
	return JSON.stringify(sortKeys(v), null, 2);
}

function sortKeys(v: unknown): unknown {
	if (Array.isArray(v)) return v.map(sortKeys);
	if (v && typeof v === 'object')
		return Object.fromEntries(
			Object.keys(v)
				.sort()
				.map((k) => [k, sortKeys((v as Record<string, unknown>)[k])])
		);
	return v;
}

/** An LLM request or response as the text a person reads: each message's
 * role and content, or the reply's content; anything else as asText. */
export function llmText(v: unknown): string {
	if (v && typeof v === 'object') {
		const o = v as { messages?: { role?: string; content?: unknown }[]; choices?: { message?: { content?: unknown } }[] };
		if (Array.isArray(o.messages)) {
			const { messages, ...rest } = o as Record<string, unknown> & { messages: { role?: string; content?: unknown }[] };
			const head = Object.keys(rest).length ? asText(rest) + '\n\n' : '';
			return head + messages.map((m) => `[${m.role ?? '?'}]\n${typeof m.content === 'string' ? m.content : asText(m.content)}`).join('\n\n');
		}
		const content = o.choices?.[0]?.message?.content;
		if (typeof content === 'string') return content;
	}
	return asText(v);
}

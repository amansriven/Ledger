import { defineConfig } from 'vitest/config';
import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';

// The trace server `npm run dev` forwards API calls to: `ledger ui` or
// `ledger-server`, both on 8700 by default.
const api = process.env.LEDGER_API ?? 'http://127.0.0.1:8700';

export default defineConfig({
	plugins: [
		sveltekit({
			compilerOptions: {
				// Force runes mode for the project, except for libraries. Can be removed in svelte 6.
				runes: ({ filename }) =>
					filename.split(/[/\\]/).includes('node_modules') ? undefined : true
			},
			// A single-page app: nothing is prerendered, and both trace servers
			// answer unknown paths with index.html.
			adapter: adapter({ fallback: 'index.html' })
		})
	],
	server: {
		proxy: { '/ledger.v1.TraceService': api }
	},
	test: {
		expect: { requireAssertions: true },
		projects: [
			{
				extends: './vite.config.ts',
				test: {
					name: 'unit',
					environment: 'node',
					include: ['src/**/*.{test,spec}.{js,ts}'],
					exclude: ['src/**/*.svelte.{test,spec}.{js,ts}']
				}
			}
		]
	}
});

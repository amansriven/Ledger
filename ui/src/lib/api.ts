// The trace service client. Both trace servers (`ledger ui` and
// `ledger-server`) serve the UI and the API from one origin; `npm run dev`
// proxies the API to one of them (see vite.config.ts).
import { createClient } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';
import { TraceService } from './gen/trace_service_pb';

export const api = createClient(
	TraceService,
	createConnectTransport({ baseUrl: globalThis.location?.origin ?? 'http://127.0.0.1:8700' })
);

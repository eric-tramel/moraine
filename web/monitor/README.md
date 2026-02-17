# Moraine Monitor Web

Local Svelte + Vite frontend for the Moraine monitor UI.

Release packaging expects built assets at `web/monitor/dist`.

## Prerequisites

- `bun` on your `PATH`
- Playwright browser binaries for e2e tests:

```bash
cd web/monitor
bunx playwright install chromium
```

## Setup

```bash
cd web/monitor
bun install --frozen-lockfile
```

## Local Development

- Start Vite dev server:

```bash
bun run dev
```

- Build production assets:

```bash
bun run build
```

- Preview the production build locally:

```bash
bun run preview -- --host 127.0.0.1 --port 4173
```

Note: the app calls `/api/*` on the same origin. When running `bun run dev`, API calls fail unless you provide a same-origin proxy/backend.

## Test Workflow

- Typecheck:

```bash
bun run typecheck
```

- Unit tests (Vitest):

```bash
bun run test
```

- Unit tests in watch mode:

```bash
bun run test:watch
```

- Playwright smoke e2e (local preview server + mocked API responses):

```bash
bun run test:e2e
```

- Playwright live e2e against a running Moraine monitor instance:

```bash
MONITOR_BASE_URL=http://127.0.0.1:8080 bun run test:e2e -- e2e/monitor.live.spec.ts
```

Optional live-test assertions can be tuned with:

- `MORAINE_E2E_CODEX_KEYWORD`
- `MORAINE_E2E_CLAUDE_KEYWORD`
- `MORAINE_E2E_CODEX_TRACE_MARKER`
- `MORAINE_E2E_CLAUDE_TRACE_MARKER`

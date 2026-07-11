import { afterEach, expect, it, vi } from 'vitest';
import type { Session } from '../types/sessions';
import { fetchSessions } from './sessions';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

it('fetches sessions from the canonical URL with the JSON header', async () => {
  const session: Session = {
    id: 'session-1',
    title: 'Canonical API session',
    harness: { id: 'codex', label: 'Codex', short: 'C', hue: 150 },
    startedAt: 1_700_000_000_000,
    endedAt: 1_700_000_001_000,
    durationMs: 1_000,
    status: 'completed',
    models: ['gpt-5'],
    turns: [],
    totalTokens: 0,
    totalToolCalls: 0,
    tags: [],
    traceId: 'trace-1',
  };
  const fetchMock = vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ ok: true, sessions: [session] }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    }),
  );
  vi.stubGlobal('fetch', fetchMock);

  await expect(fetchSessions({ allowMock: false })).resolves.toEqual([session]);
  expect(fetchMock).toHaveBeenCalledOnce();
  expect(fetchMock).toHaveBeenCalledWith('/api/v1/sessions', {
    headers: { Accept: 'application/json' },
  });
});

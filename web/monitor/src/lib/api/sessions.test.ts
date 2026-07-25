import { afterEach, describe, expect, it, vi } from 'vitest';
import type { SessionSummary, SessionTurn } from '../types/sessions';
import { MonitorApiError, fetchSessionPage, fetchSessions } from './sessions';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function summary(id: string): SessionSummary {
  return {
    id,
    title: 'Canonical API session',
    displayLabel: 'Canonical API session',
    harness: 'codex',
    source: 'ci-codex',
    inferenceProvider: 'openai',
    mode: 'tool_calling',
    startedAt: 1_700_000_000_000,
    endedAt: 1_700_000_001_000,
    status: 'completed',
    turnCount: 3,
    eventCount: 12,
    toolCallCount: 2,
    sessionSlug: 'canonical-api-session',
    sessionSummary: 'Inspected the repository',
  };
}

function turn(turnSeq: number): SessionTurn {
  return {
    turnSeq,
    turnId: `turn-${turnSeq}`,
    startedAt: 1_700_000_000_000,
    endedAt: 1_700_000_001_000,
    eventCount: 4,
    userMessages: 1,
    assistantMessages: 1,
    toolCalls: 1,
    toolResults: 1,
    reasoningItems: 0,
    userInput: `prompt ${turnSeq}`,
    finalResponse: `reply ${turnSeq}`,
    toolsCalled: ['Read'],
    completed: true,
  };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function requestedUrl(fetchMock: ReturnType<typeof vi.fn>, call = 0): URL {
  return new URL(fetchMock.mock.calls[call][0] as string, 'http://monitor.test');
}

describe('fetchSessions', () => {
  it('sends the paging and filter parameters and returns the page envelope', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        ok: true,
        read_model: 'live',
        sessions: [summary('session-1')],
        limit: 25,
        next_cursor: 'cursor-page-2',
        has_more: true,
        window: { start: 10, end: 20 },
      }),
    );
    vi.stubGlobal('fetch', fetchMock);

    const page = await fetchSessions({ since: '7d', limit: 50, harness: 'codex', sort: 'desc' });

    expect(page.sessions).toEqual([summary('session-1')]);
    expect(page.nextCursor).toBe('cursor-page-2');
    expect(page.hasMore).toBe(true);
    expect(page.window).toEqual({ start: 10, end: 20 });
    // The server's effective page size, which may sit below the requested limit.
    expect(page.limit).toBe(25);

    const url = requestedUrl(fetchMock);
    expect(url.pathname).toBe('/api/v1/sessions');
    expect(url.searchParams.get('limit')).toBe('50');
    expect(url.searchParams.get('since')).toBe('7d');
    expect(url.searchParams.get('harness')).toBe('codex');
    expect(url.searchParams.get('sort')).toBe('desc');
    // A cleared filter is absent, not an empty value that would match nothing.
    expect(url.searchParams.has('source')).toBe(false);
    expect(url.searchParams.has('cursor')).toBe(false);
    expect(fetchMock).toHaveBeenCalledWith(expect.any(String), {
      headers: { Accept: 'application/json' },
    });
  });

  it('carries a continuation cursor verbatim', async () => {
    const cursor = 'eyJ2ZXJzaW9uIjoxfQ';
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ ok: true, sessions: [], next_cursor: null, has_more: false }));
    vi.stubGlobal('fetch', fetchMock);

    const page = await fetchSessions({ cursor });

    expect(requestedUrl(fetchMock).searchParams.get('cursor')).toBe(cursor);
    expect(page.nextCursor).toBeNull();
    expect(page.hasMore).toBe(false);
  });

  it('treats an empty page carrying a cursor as "keep paging"', async () => {
    // The repository's legal signal that its candidate budget ran out before
    // anything survived. Reading it as "no results" would stop the traversal
    // short of sessions that exist.
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        jsonResponse({ ok: true, sessions: [], next_cursor: 'keep-going', has_more: true }),
      ),
    );

    const page = await fetchSessions();

    expect(page.sessions).toEqual([]);
    expect(page.hasMore).toBe(true);
    expect(page.nextCursor).toBe('keep-going');
  });

  it('throws with the server classification rather than substituting sessions', async () => {
    // The mock-session fallback this replaced rendered fabricated sessions on
    // every failure and left the error store null, so an outage and an idle
    // store looked identical.
    for (const [status, code] of [
      [504, 'deadline_exceeded'],
      [429, 'resource_exhausted'],
      [400, 'invalid_cursor'],
    ] as const) {
      vi.stubGlobal(
        'fetch',
        vi.fn().mockResolvedValue(jsonResponse({ ok: false, error: `boom ${code}`, code }, status)),
      );

      const error = await fetchSessions().then(
        () => {
          throw new Error(`expected ${status} to reject`);
        },
        (rejection: unknown) => rejection,
      );
      expect(error).toBeInstanceOf(MonitorApiError);
      expect((error as MonitorApiError).status).toBe(status);
      expect((error as MonitorApiError).code).toBe(code);
      expect((error as MonitorApiError).message).toBe(`boom ${code}`);
    }
  });

  it('rejects a transport failure instead of resolving with generated data', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('network down')));
    await expect(fetchSessions()).rejects.toThrow('network down');
  });
});

describe('fetchSessionPage', () => {
  it('requests one bounded page of turns for the named session', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        ok: true,
        read_model: 'live',
        limit: 25,
        session: { ...summary('session 1'), completed: true, turns: [turn(1), turn(2)] },
        has_more: true,
        next_cursor: 'turn-cursor',
      }),
    );
    vi.stubGlobal('fetch', fetchMock);

    const page = await fetchSessionPage('session 1', { limit: 25 });

    expect(page.turns.map((t) => t.turnSeq)).toEqual([1, 2]);
    expect(page.nextCursor).toBe('turn-cursor');
    expect(page.hasMore).toBe(true);
    expect(page.reopen).toBe(false);

    const url = requestedUrl(fetchMock);
    // The id is a path segment and must survive characters a path would eat.
    expect(url.pathname).toBe('/api/v1/sessions/session%201/page');
    expect(url.searchParams.get('limit')).toBe('25');
  });

  it('continues a traversal with the cursor the previous page minted', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ ok: true, session: { ...summary('s'), completed: true, turns: [turn(3)] } }),
      );
    vi.stubGlobal('fetch', fetchMock);

    const page = await fetchSessionPage('s', { cursor: 'turn-cursor' });

    expect(requestedUrl(fetchMock).searchParams.get('cursor')).toBe('turn-cursor');
    expect(page.hasMore).toBe(false);
    expect(page.nextCursor).toBeNull();
  });

  it('surfaces a reopen rather than presenting a stale page as complete', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(jsonResponse({ ok: true, read_model: 'live', reopen: true })),
    );

    const page = await fetchSessionPage('session-1');

    expect(page.reopen).toBe(true);
    expect(page.turns).toEqual([]);
    expect(page.hasMore).toBe(false);
  });

  it('throws when the backend has not published its canonical read indexes', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        jsonResponse(
          { ok: false, error: 'transcripts unavailable', code: 'canonical_reader_unavailable' },
          503,
        ),
      ),
    );

    const error = await fetchSessionPage('session-1').then(
      () => {
        throw new Error('expected 503 to reject');
      },
      (rejection: unknown) => rejection,
    );
    expect((error as MonitorApiError).code).toBe('canonical_reader_unavailable');
  });
});

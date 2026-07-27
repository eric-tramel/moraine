import type {
  SessionPageResponse,
  SessionSearchResponse,
  SessionSummary,
  SessionTranscript,
  SessionTurn,
  SessionWindow,
  SessionsResponse,
} from '../types/sessions';

/**
 * The session feed and the lazy transcript load.
 *
 * There is deliberately no fallback to generated data. Every failure here —
 * a deadline, an exhausted budget, a refused cursor, an unreachable backend —
 * throws, so the dashboard renders an error. Substituting fabricated sessions
 * would make an outage indistinguishable from an idle store.
 */

/** The default page size. The route clamps to 1..=200 and then to the backend's `max_results`. */
const DEFAULT_LIMIT = 50;

export interface FetchSessionsParams {
  /** `1h`|`6h`|`24h`|`7d`|`30d`|`90d`|`all`. The server falls back to `30d`. */
  since?: string;
  limit?: number;
  /** A `next_cursor` from a previous page of the SAME filter and `since`. */
  cursor?: string | null;
  harness?: string | null;
  source?: string | null;
  mode?: string | null;
  sort?: 'desc' | 'asc';
}

export interface SessionsPage {
  sessions: SessionSummary[];
  nextCursor: string | null;
  /** Exactly `nextCursor !== null`. An empty page with `hasMore` means "keep paging". */
  hasMore: boolean;
  /** The window this page was computed under; a continuation replays it. */
  window: SessionWindow | null;
  /** The effective page size the server applied, which may be below `limit`. */
  limit: number;
}

export interface FetchSessionPageParams {
  limit?: number;
  /** A `next_cursor` from a previous page of the same session. */
  cursor?: string | null;
}

export interface SearchSessionsParams {
  harness?: string | null;
  source?: string | null;
  limit?: number;
}

/**
 * The four bounded-answer signals, kept distinct because they have different
 * remedies. `truncated` is answered by raising `limit`; `hitsTruncated`,
 * `incomplete` and `dropped` are not.
 */
export interface SessionSearchPage {
  /** The query the server actually ranked, after trimming. */
  query: string;
  /** Ranked best-first. The SAME summary objects `fetchSessions` returns. */
  sessions: SessionSummary[];
  /** SESSION grain: more ranked sessions existed than the bound returned. */
  truncated: boolean;
  /**
   * HIT grain: the ranking filled its event-hit budget, so matching events
   * existed that it never examined. A term matching one session thirty times
   * sets this and leaves `truncated` false.
   */
  hitsTruncated: boolean;
  /** The ranking's bounded candidate window was exhausted first (#597 §1.6). */
  incomplete: boolean;
  /**
   * Ranked sessions were removed by the server's exact post-ranking re-check
   * and nothing refilled them, so this answer is a strict subset of what was
   * ranked rather than everything that matched.
   */
  dropped: boolean;
}

/** Ranked sessions per search. The route clamps to `1..=50`, then to the backend's `max_results`. */
const DEFAULT_SEARCH_LIMIT = 25;

export interface SessionTurnPage {
  /** Absent when the server asked for a reopen. */
  sessionId: string | null;
  header: SessionTranscript | null;
  turns: SessionTurn[];
  nextCursor: string | null;
  hasMore: boolean;
  /** The pinned view changed underneath the cursor; discard and reload page 1. */
  reopen: boolean;
}

interface FailurePayload {
  error?: string;
  code?: string;
}

/** A monitor failure that carries the server's machine-readable classification. */
export class MonitorApiError extends Error {
  readonly status: number;
  readonly code: string | null;

  constructor(message: string, status: number, code: string | null) {
    super(message);
    this.name = 'MonitorApiError';
    this.status = status;
    this.code = code;
  }
}

async function failure(response: Response): Promise<MonitorApiError> {
  let payload: FailurePayload = {};
  if ((response.headers.get('content-type') ?? '').includes('application/json')) {
    try {
      payload = (await response.json()) as FailurePayload;
    } catch {
      payload = {};
    }
  }
  return new MonitorApiError(
    payload.error || `request failed (${response.status})`,
    response.status,
    payload.code ?? null,
  );
}

function appendOptional(query: URLSearchParams, key: string, value: string | null | undefined): void {
  const trimmed = value?.trim();
  if (trimmed) {
    query.set(key, trimmed);
  }
}

export async function fetchSessions(params: FetchSessionsParams = {}): Promise<SessionsPage> {
  const limit = params.limit ?? DEFAULT_LIMIT;
  const query = new URLSearchParams({ limit: String(limit) });
  appendOptional(query, 'since', params.since);
  appendOptional(query, 'cursor', params.cursor);
  appendOptional(query, 'harness', params.harness);
  appendOptional(query, 'source', params.source);
  appendOptional(query, 'mode', params.mode);
  appendOptional(query, 'sort', params.sort);

  const response = await fetch(`/api/v1/sessions?${query.toString()}`, {
    headers: { Accept: 'application/json' },
  });
  if (!response.ok) {
    throw await failure(response);
  }

  const data = (await response.json()) as SessionsResponse;
  if (!data.ok || !Array.isArray(data.sessions)) {
    throw new MonitorApiError(data.error || 'malformed sessions response', response.status, data.code ?? null);
  }

  const nextCursor = data.next_cursor ?? null;
  return {
    sessions: data.sessions,
    nextCursor,
    // Trust the server's own answer when it gives one; `has_more` is defined as
    // `next_cursor != null`, so the fallback below cannot disagree with it.
    hasMore: data.has_more ?? nextCursor !== null,
    window: data.window ?? null,
    limit: data.limit ?? limit,
  };
}

/**
 * Whole-corpus session search.
 *
 * This is what replaced the page-local `query` filter (issue-599 WI-09). The
 * server ranks message content across every session this backend may serve, and
 * returns session SUMMARIES — no snippets, no transcripts — so the response
 * stays flat as the corpus grows and a result opens through the same
 * `fetchSessionPage` reader a listed session does.
 *
 * The caller is responsible for debouncing; every call is a real backend
 * ranking.
 */
export async function searchSessions(
  query: string,
  params: SearchSessionsParams = {},
): Promise<SessionSearchPage> {
  const search = new URLSearchParams({
    q: query.trim(),
    limit: String(params.limit ?? DEFAULT_SEARCH_LIMIT),
  });
  appendOptional(search, 'harness', params.harness);
  appendOptional(search, 'source', params.source);

  const response = await fetch(`/api/v1/sessions/search?${search.toString()}`, {
    headers: { Accept: 'application/json' },
  });
  if (!response.ok) {
    throw await failure(response);
  }

  const data = (await response.json()) as SessionSearchResponse;
  if (!data.ok || !Array.isArray(data.sessions)) {
    throw new MonitorApiError(
      data.error || 'malformed session search response',
      response.status,
      data.code ?? null,
    );
  }

  return {
    query: data.query ?? query.trim(),
    // Ranked order is the server's answer. Never re-sort it here: relevance is
    // the whole point of the route, and recency ordering would discard it.
    sessions: data.sessions,
    truncated: data.truncated ?? false,
    hitsTruncated: data.hits_truncated ?? false,
    incomplete: data.incomplete ?? false,
    dropped: data.dropped ?? false,
  };
}

export async function fetchSessionPage(
  sessionId: string,
  params: FetchSessionPageParams = {},
): Promise<SessionTurnPage> {
  const query = new URLSearchParams({ limit: String(params.limit ?? DEFAULT_LIMIT) });
  appendOptional(query, 'cursor', params.cursor);

  const response = await fetch(
    `/api/v1/sessions/${encodeURIComponent(sessionId)}/page?${query.toString()}`,
    { headers: { Accept: 'application/json' } },
  );
  if (!response.ok) {
    throw await failure(response);
  }

  const data = (await response.json()) as SessionPageResponse;
  if (!data.ok) {
    throw new MonitorApiError(data.error || 'malformed session page response', response.status, data.code ?? null);
  }
  if (data.reopen) {
    return { sessionId: null, header: null, turns: [], nextCursor: null, hasMore: false, reopen: true };
  }
  if (!data.session || !Array.isArray(data.session.turns)) {
    throw new MonitorApiError('session page carried no turns', response.status, data.code ?? null);
  }

  const nextCursor = data.next_cursor ?? null;
  return {
    sessionId: data.session.id,
    header: data.session,
    turns: data.session.turns,
    nextCursor,
    hasMore: data.has_more ?? nextCursor !== null,
    reopen: false,
  };
}

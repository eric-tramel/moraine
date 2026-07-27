/**
 * The dashboard's view of `/api/v1/sessions` and `/api/v1/sessions/:id/page`.
 *
 * The session feed carries SUMMARIES ONLY — navigation scalars and labels, no
 * message content — so its size stays flat as transcripts grow. Turns arrive
 * only when a session is opened, one bounded page at a time. Nothing here has a
 * `steps` shape: the server has no per-step read path, and inventing one on the
 * client would mean fetching transcripts to render a list.
 */

/** The only two values the server derives (`completed` flag, else recency). */
export type SessionStatus = 'active' | 'completed';

/** A harness identity for display, derived from the summary's harness id. */
export interface Harness {
  id: string;
  label: string;
  short: string;
  hue: number;
}

/** One row of `/api/v1/sessions`. */
export interface SessionSummary {
  id: string;
  title: string | null;
  displayLabel: string;
  harness: string | null;
  source: string | null;
  inferenceProvider: string | null;
  mode: string;
  startedAt: number;
  endedAt: number;
  status: SessionStatus;
  turnCount: number;
  eventCount: number;
  toolCallCount: number;
  sessionSlug: string | null;
  sessionSummary: string | null;
}

export interface SessionWindow {
  start: number;
  end: number;
}

export interface SessionsResponse {
  ok: boolean;
  read_model?: 'live';
  sessions?: SessionSummary[];
  limit?: number;
  next_cursor?: string | null;
  has_more?: boolean;
  window?: SessionWindow;
  error?: string;
  code?: string;
}

/**
 * `/api/v1/sessions/search` — whole-corpus content search.
 *
 * `sessions` are the SAME summary objects the feed returns, in ranked order,
 * so a result opens through `/api/v1/sessions/:id/page` like any other row.
 * There is no cursor: a relevance ranking is bounded, not paged.
 */
export interface SessionSearchResponse {
  ok: boolean;
  read_model?: 'live';
  query?: string;
  terms?: string[];
  sessions?: SessionSummary[];
  limit?: number;
  result_count?: number;
  /** SESSION grain: more ranked sessions existed than the bound returned. */
  truncated?: boolean;
  /** HIT grain: the ranking filled its event-hit budget. */
  hits_truncated?: boolean;
  /** The ranking's bounded candidate window was exhausted first. */
  incomplete?: boolean;
  /** Ranked sessions were removed by the exact re-check and not refilled. */
  dropped?: boolean;
  error?: string;
  code?: string;
}

/** One turn from `/api/v1/sessions/:id/page`: counts, references, summaries. */
export interface SessionTurn {
  turnSeq: number;
  turnId: string;
  startedAt: number;
  endedAt: number;
  eventCount: number;
  userMessages: number;
  assistantMessages: number;
  toolCalls: number;
  toolResults: number;
  reasoningItems: number;
  userInput: string | null;
  finalResponse: string | null;
  toolsCalled: string[];
  completed: boolean;
}

/** The opened session header plus the turns loaded so far. */
export interface SessionTranscript {
  id: string;
  title: string | null;
  harness: string | null;
  source: string | null;
  inferenceProvider: string | null;
  mode: string;
  startedAt: number;
  endedAt: number;
  completed: boolean;
  turnCount: number;
  eventCount: number;
  sessionSlug: string | null;
  sessionSummary: string | null;
  turns: SessionTurn[];
}

export interface SessionPageResponse {
  ok: boolean;
  read_model?: 'live';
  limit?: number;
  session?: SessionTranscript;
  /** The pinned view changed underneath the cursor; reload from page 1. */
  reopen?: boolean;
  has_more?: boolean;
  next_cursor?: string | null;
  error?: string;
  code?: string;
}

/**
 * The session-panel controls.
 *
 * `query` is NOT a client-side filter. It is the whole-corpus search the server
 * runs (`/api/v1/sessions/search`), debounced by the panel; only `status` is
 * narrowed on what is already rendered, and `harness` is applied both locally
 * (for instant feedback) and server-side (which is what makes it correct across
 * pages and across a search).
 */
export interface SessionsFilter {
  query: string;
  status: string;
  harness: string;
}

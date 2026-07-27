import { derived, writable } from 'svelte/store';
import { MonitorApiError } from '../api/sessions';
import type { SessionSummary, SessionsFilter } from '../types/sessions';

export const sessionsStore = writable<SessionSummary[]>([]);
export const sessionsLoadingStore = writable<boolean>(false);
export const sessionsLoadingMoreStore = writable<boolean>(false);
export const sessionsErrorStore = writable<string | null>(null);

/**
 * The continuation for the next page, and whether one exists.
 *
 * `hasMore` is the server's own answer, so the list can never present a loaded
 * subset as the whole corpus. An empty page carrying a cursor is a legal "keep
 * paging" signal, not "no results".
 */
export const sessionsCursorStore = writable<string | null>(null);
export const sessionsHasMoreStore = writable<boolean>(false);

export const sessionsFilterStore = writable<SessionsFilter>({
  query: '',
  status: 'all',
  harness: 'all',
});

/**
 * Why a search produced no results.
 *
 * `invalid` is the server rejecting the QUERY (HTTP 400): the backend owns the
 * tokenizer, so it — not this client — decides that a string carries no
 * searchable term, and it is a hint to retype, not an outage. `unavailable` is
 * everything else: a deadline, an exhausted budget, an unreachable store.
 * Rendering the first as the second told a reader the store was down when they
 * had typed one character.
 */
export type SessionSearchFailure = 'invalid' | 'unavailable';

/**
 * The state of the whole-corpus search (issue-599 WI-09).
 *
 * `results` is `null` when no search is in effect **and when one failed** —
 * deliberately distinct from `[]`, which means "the corpus was searched and
 * nothing matched". A failed search that set `[]` would make the panel print
 * "No sessions match this search." and "0 results" beside the failure banner,
 * which is exactly the false statement the server's blank-`q` refusal exists to
 * prevent.
 */
export interface SessionSearchState {
  /** The query these results belong to; empty when no search is in effect. */
  query: string;
  results: SessionSummary[] | null;
  loading: boolean;
  error: string | null;
  /** How to render `error`; `null` exactly when `error` is `null`. */
  errorKind: SessionSearchFailure | null;
  /** SESSION grain: more ranked sessions existed than the server returned. */
  truncated: boolean;
  /** HIT grain: the ranking filled its event-hit budget. */
  hitsTruncated: boolean;
  /** The ranking's bounded candidate window was exhausted first (#597 §1.6). */
  incomplete: boolean;
  /** The server's exact re-check removed ranked sessions and did not refill. */
  dropped: boolean;
}

export const idleSessionSearch: SessionSearchState = {
  query: '',
  results: null,
  loading: false,
  error: null,
  errorKind: null,
  truncated: false,
  hitsTruncated: false,
  incomplete: false,
  dropped: false,
};

export const sessionSearchStore = writable<SessionSearchState>({ ...idleSessionSearch });

/**
 * The state a FAILED search must produce.
 *
 * Extracted so the two decisions in it are testable rather than buried in a
 * component's `catch`:
 *
 * 1. **`results` stays `null`.** A failed search searched nothing. Setting `[]`
 *    puts the panel into the "the corpus was searched and nothing matched"
 *    state, which renders "No sessions match this search." and "0 results"
 *    beside the failure banner — the corpus-wide claim the server refuses to
 *    make even for a blank query.
 * 2. **A `400` is a hint, not an outage.** The backend owns the tokenizer, so
 *    it decides that a query carries no searchable term, and it is the one that
 *    writes the message. This client renders that message as-is and does not
 *    restate or re-derive the rule; a local copy of the tokenizer's rules would
 *    drift from the server's.
 */
export function failedSessionSearch(query: string, error: unknown): SessionSearchState {
  const invalid = error instanceof MonitorApiError && error.status === 400;
  const message = error instanceof Error ? error.message : String(error);
  return {
    ...idleSessionSearch,
    query,
    results: null,
    error: invalid ? message : `Search unavailable: ${message}`,
    errorKind: invalid ? 'invalid' : 'unavailable',
  };
}

export const activeSessionIdStore = writable<string | null>(null);

/**
 * Narrow a rendered set of sessions by the controls the CLIENT can answer.
 *
 * `status` is derived from fields the summary already carries, so applying it
 * here costs nothing and is exact. `harness` is applied here for instant
 * feedback and is ALSO pushed to the server, which is what makes it correct
 * beyond the loaded page.
 *
 * `query` is deliberately absent. It is a whole-corpus server search
 * (`/api/v1/sessions/search`), and re-applying it locally would drop exactly
 * the results the search exists to find: a session that matched on a message
 * body carries that text in no field the client ever sees.
 */
export function narrowSessions(
  sessions: SessionSummary[],
  filter: SessionsFilter,
): SessionSummary[] {
  return sessions.filter((session) => {
    if (filter.status !== 'all' && session.status !== filter.status) return false;
    if (filter.harness !== 'all' && (session.harness ?? '') !== filter.harness) return false;
    return true;
  });
}

/**
 * What the panel renders: the ranked search results when a search is in effect,
 * otherwise the time-ordered feed.
 *
 * Search results keep the server's ranked order — re-sorting them by recency
 * would discard the relevance the route was called for. The feed keeps its
 * recency order.
 */
export const filteredSessionsStore = derived(
  [sessionsStore, sessionSearchStore, sessionsFilterStore],
  ([$sessions, $search, $filter]) => {
    if ($search.results !== null) {
      return narrowSessions($search.results, $filter);
    }
    const narrowed = narrowSessions($sessions, $filter);
    return [...narrowed].sort((a, b) => b.endedAt - a.endedAt);
  },
);

/** What the filter bar's count is describing. */
export interface SessionCountLabelInput {
  /** Rows actually on screen, i.e. AFTER `narrowSessions`. */
  rendered: number;
  /**
   * Rows the SERVER answered with, before any local narrowing; `null` when no
   * search is in effect. This is the population every bounded-answer flag below
   * describes.
   */
  serverResults: number | null;
  searchLoading: boolean;
  searchFailed: boolean;
  truncated: boolean;
  hitsTruncated: boolean;
  incomplete: boolean;
  dropped: boolean;
}

/**
 * The filter bar's count label, which must never overstate what it counts.
 *
 * The feed is paged and reports no corpus total, so "loaded" — never a
 * `{count} / {total}` ratio, which would invent a denominator.
 *
 * A search DOES have a denominator the server emitted: the number of sessions
 * it answered with. That matters because the four bounded-answer flags are
 * facts about the SERVER's answer, while the rows on screen have additionally
 * been narrowed by `status` locally. Attaching a qualifier to a locally
 * narrowed count asserts something about a population the ranking never
 * emitted: with `status: 'active'` and a server answer of 25 of which 3 are
 * active, "3 results (top matches)" claims the ranking found more than those
 * 3 — but 22 were hidden here, not truncated there. So:
 *
 * * nothing narrowed locally — `N results` plus the qualifier, and `N` IS the
 *   server's answer;
 * * narrowed locally — `M of N results`, with NO qualifier. The denominator is
 *   the server's own count rather than an invented one, and the qualifier is
 *   dropped rather than misattributed.
 *
 * The qualifier itself distinguishes the two things the server reports
 * separately, because they have different remedies:
 *
 * * **top matches** — the ranking found more than it returned, at the SESSION
 *   grain (`truncated`) or the event-HIT grain (`hitsTruncated`). More exists;
 *   a wider request reaches further into it.
 * * **partial** — the answer is shorter than it should be: the ranking's
 *   candidate window was exhausted (`incomplete`) or the server's exact
 *   re-check removed ranked sessions and refilled nothing (`dropped`). A wider
 *   request fixes neither, so this must not read as "top matches", which
 *   invites exactly that.
 *
 * A FAILED search counts nothing at all. It never searched the corpus, so
 * "0 results" would be a claim about the corpus and "N loaded" would label rows
 * that are not on screen.
 */
export function sessionCountLabel(input: SessionCountLabelInput): string {
  if (input.searchFailed) return 'search did not run';
  if (input.searchLoading) return 'searching…';
  if (input.serverResults === null) return `${input.rendered} loaded`;

  if (input.rendered !== input.serverResults) {
    return `${input.rendered} of ${input.serverResults} results`;
  }
  const qualifier =
    input.truncated || input.hitsTruncated
      ? ' (top matches)'
      : input.incomplete || input.dropped
        ? ' (partial)'
        : '';
  return `${input.rendered} result${input.rendered === 1 ? '' : 's'}${qualifier}`;
}

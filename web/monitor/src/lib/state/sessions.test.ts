import { get } from 'svelte/store';
import { afterEach, describe, expect, it } from 'vitest';
import { MonitorApiError } from '../api/sessions';
import type { SessionSummary, SessionsFilter } from '../types/sessions';
import {
  failedSessionSearch,
  filteredSessionsStore,
  idleSessionSearch,
  narrowSessions,
  sessionCountLabel,
  sessionSearchStore,
  sessionsFilterStore,
  sessionsStore,
  type SessionCountLabelInput,
} from './sessions';

function summary(overrides: Partial<SessionSummary> = {}): SessionSummary {
  return {
    id: 'session-1',
    title: 'Inspect the repository',
    displayLabel: 'Inspect the repository',
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
    sessionSlug: 'inspect-the-repository',
    sessionSummary: 'Walked the workspace manifest',
    ...overrides,
  };
}

function filter(overrides: Partial<SessionsFilter> = {}): SessionsFilter {
  return { query: '', status: 'all', harness: 'all', ...overrides };
}

afterEach(() => {
  sessionsStore.set([]);
  sessionSearchStore.set({ ...idleSessionSearch });
  sessionsFilterStore.set(filter());
});

describe('narrowSessions', () => {
  it('narrows by the two statuses the server can produce', () => {
    const sessions = [
      summary({ id: 'a', status: 'active' }),
      summary({ id: 'b', status: 'completed' }),
    ];
    expect(narrowSessions(sessions, filter({ status: 'active' })).map((s) => s.id)).toEqual(['a']);
    expect(narrowSessions(sessions, filter({ status: 'completed' })).map((s) => s.id)).toEqual([
      'b',
    ]);
  });

  it('treats a session with no recorded harness as matching no harness filter', () => {
    const sessions = [summary({ id: 'a', harness: null }), summary({ id: 'b', harness: 'codex' })];
    expect(narrowSessions(sessions, filter({ harness: 'codex' })).map((s) => s.id)).toEqual(['b']);
  });

  it('does not filter on the query, because the query is a server search', () => {
    // The deleted page-local branch matched title/label/slug/id. Re-applying it
    // to a ranked result set would drop every session that matched on message
    // content — precisely the set whole-corpus search exists to find, and whose
    // matching text appears in no field the client ever receives.
    const sessions = [
      summary({ id: 'a', title: 'Inspect the repository' }),
      summary({
        id: 'b',
        title: 'Unrelated title',
        displayLabel: 'Unrelated title',
        sessionSlug: null,
        sessionSummary: null,
      }),
    ];
    expect(
      narrowSessions(sessions, filter({ query: 'nothing here matches any label' })).map((s) => s.id),
    ).toEqual(['a', 'b']);
  });
});

describe('filteredSessionsStore', () => {
  it('renders the time-ordered feed, newest first, when no search is in effect', () => {
    sessionsStore.set([
      summary({ id: 'older', endedAt: 1_700_000_000_000 }),
      summary({ id: 'newer', endedAt: 1_700_000_009_000 }),
    ]);
    expect(get(filteredSessionsStore).map((s) => s.id)).toEqual(['newer', 'older']);
  });

  it('renders search results in the order the server ranked them', () => {
    // The feed sorts by recency. A ranking does not, and re-sorting it would
    // throw away the relevance the search was issued for: the best match here
    // is deliberately the OLDEST session.
    sessionsStore.set([summary({ id: 'feed-row' })]);
    sessionSearchStore.set({
      ...idleSessionSearch,
      query: 'projection',
      results: [
        summary({ id: 'best-match', endedAt: 1_600_000_000_000 }),
        summary({ id: 'second', endedAt: 1_700_000_009_000 }),
      ],
    });
    expect(get(filteredSessionsStore).map((s) => s.id)).toEqual(['best-match', 'second']);
  });

  it('distinguishes an unsearched panel from a search that matched nothing', () => {
    sessionsStore.set([summary({ id: 'feed-row' })]);

    // `null` results: no search in effect, so the feed shows.
    expect(get(filteredSessionsStore).map((s) => s.id)).toEqual(['feed-row']);

    // `[]` results: the corpus WAS searched and nothing matched. Falling back
    // to the feed here would present unrelated sessions as the answer.
    sessionSearchStore.set({ ...idleSessionSearch, query: 'nothing', results: [] });
    expect(get(filteredSessionsStore)).toEqual([]);
  });

  // A FAILED search searched nothing, so it is neither of the two states above.
  // Setting `results` to `[]` here is what made the panel print "No sessions
  // match this search." beside "Search unavailable" — the corpus-wide claim the
  // server refuses to make even for a blank query.
  //
  // MUTATION: return `results: []` from `failedSessionSearch`; this fails.
  it('a failed search is not a search that matched nothing', () => {
    const state = failedSessionSearch(
      'projection',
      new MonitorApiError('deadline', 504, 'deadline_exceeded'),
    );
    expect(state.results).toBeNull();
    expect(state.error).toBe('Search unavailable: deadline');
    expect(state.errorKind).toBe('unavailable');

    // And the panel's own "is a search in effect" predicate reads it as "no",
    // so the ranked-results empty state is unreachable from a failure.
    sessionsStore.set([summary({ id: 'feed-row' })]);
    sessionSearchStore.set(state);
    expect(get(sessionSearchStore).results === null).toBe(true);
  });

  // The backend owns the tokenizer, so "your query has no searchable term" is
  // its 400, not an outage. Rendering it as one told a reader the store was
  // down when they had typed one character, permanently and unrecoverably.
  //
  // MUTATION: drop the `error.status === 400` branch in `failedSessionSearch`;
  // this fails on both the kind and the un-prefixed message.
  it('classifies a rejected query as a hint and everything else as an outage', () => {
    const rejected = failedSessionSearch(
      'x',
      new MonitorApiError(
        'session search failed: query has no searchable terms',
        400,
        'invalid_argument',
      ),
    );
    expect(rejected.errorKind).toBe('invalid');
    // Rendered verbatim: the rule belongs to the server and is not restated
    // here, so no local copy can drift from it.
    expect(rejected.error).toBe('session search failed: query has no searchable terms');
    expect(rejected.error).not.toContain('unavailable');
    expect(rejected.results).toBeNull();

    for (const status of [429, 503, 504]) {
      const outage = failedSessionSearch('projection', new MonitorApiError('boom', status, null));
      expect(outage.errorKind, `status=${status}`).toBe('unavailable');
      expect(outage.error, `status=${status}`).toBe('Search unavailable: boom');
    }

    // A transport failure is not a `MonitorApiError` at all.
    const offline = failedSessionSearch('projection', new TypeError('Failed to fetch'));
    expect(offline.errorKind).toBe('unavailable');
    expect(offline.error).toBe('Search unavailable: Failed to fetch');
  });

  it('still applies the client-answerable filters to search results', () => {
    sessionSearchStore.set({
      ...idleSessionSearch,
      query: 'projection',
      results: [
        summary({ id: 'active-hit', status: 'active' }),
        summary({ id: 'completed-hit', status: 'completed' }),
      ],
    });
    sessionsFilterStore.set(filter({ query: 'projection', status: 'active' }));
    expect(get(filteredSessionsStore).map((s) => s.id)).toEqual(['active-hit']);
  });
});

describe('sessionCountLabel', () => {
  function label(overrides: Partial<SessionCountLabelInput> = {}): string {
    return sessionCountLabel({
      rendered: 0,
      serverResults: null,
      searchLoading: false,
      searchFailed: false,
      truncated: false,
      hitsTruncated: false,
      incomplete: false,
      dropped: false,
      ...overrides,
    });
  }

  it('counts the paged feed as "loaded" and invents no denominator', () => {
    // The feed reports no corpus total, so a `{count} / {total}` ratio here
    // would be a number the server never emitted.
    expect(label({ rendered: 2 })).toBe('2 loaded');
    expect(label({ rendered: 0 })).toBe('0 loaded');
  });

  it('qualifies a search only when the qualifier and the count describe one population', () => {
    // Nothing narrowed locally: the rows on screen ARE the server's answer, so
    // the server's fact about that answer attaches to the number shown.
    expect(label({ rendered: 3, serverResults: 3, truncated: true })).toBe('3 results (top matches)');
    expect(label({ rendered: 1, serverResults: 1, hitsTruncated: true })).toBe('1 result (top matches)');
    expect(label({ rendered: 4, serverResults: 4, incomplete: true })).toBe('4 results (partial)');
    expect(label({ rendered: 4, serverResults: 4, dropped: true })).toBe('4 results (partial)');
    expect(label({ rendered: 2, serverResults: 2 })).toBe('2 results');
  });

  // THE GUARD. `status` narrows locally; `truncated` is a fact about what the
  // RANKING returned. Printing "3 results (top matches)" when the server
  // answered 25 and the status filter hid 22 asserts the ranking found more
  // than those 3 — against a denominator the ranking never emitted, with a
  // remedy ("raise the limit") that would not bring the 22 back.
  //
  // MUTATION: drop the `rendered !== serverResults` branch from
  // `sessionCountLabel`; this fails.
  //
  // The WIRING is a different mutation with a different guard: passing `count`
  // for `serverResults` in `FilterBar.svelte` leaves this file green — the
  // label is a pure function and this test calls it directly — and is caught
  // instead by `a failed search is reported, never answered from the loaded
  // page` in the mocked Playwright suite. Both were run; neither covers the
  // other.
  it('never attaches a server qualifier to a locally narrowed count', () => {
    expect(
      label({ rendered: 3, serverResults: 25, truncated: true, hitsTruncated: true }),
    ).toBe('3 of 25 results');
    expect(label({ rendered: 0, serverResults: 25, dropped: true })).toBe('0 of 25 results');
    // The denominator is the server's own count, so it is not invented.
    expect(label({ rendered: 1, serverResults: 2, incomplete: true })).toBe('1 of 2 results');
  });

  it('counts nothing at all for a search that is running or failed', () => {
    // A failed search searched nothing: "0 results" would be a claim about the
    // corpus and "N loaded" would label rows that are not on screen.
    expect(label({ rendered: 7, serverResults: null, searchFailed: true })).toBe(
      'search did not run',
    );
    // …including when a previous answer is still in `results`.
    expect(label({ rendered: 7, serverResults: 7, searchFailed: true })).toBe('search did not run');
    expect(label({ rendered: 7, serverResults: 7, searchLoading: true })).toBe('searching…');
  });
});

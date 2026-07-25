import { derived, writable } from 'svelte/store';
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

export const activeSessionIdStore = writable<string | null>(null);

/**
 * Narrow the sessions ALREADY LOADED.
 *
 * `query` matches labels and identifiers only. Message content is not in the
 * feed and is not fetched to answer a keystroke, so this cannot search
 * transcripts — the filter input says so. Whole-corpus search arrives with
 * issue #597; until then this is page-local by construction.
 *
 * `harness` and `status` are applied here too so a filter change is instant on
 * what is loaded; `harness` is additionally pushed to the server, which is what
 * makes it correct across pages.
 */
export function filterSessions(
  sessions: SessionSummary[],
  filter: SessionsFilter,
): SessionSummary[] {
  const q = filter.query.trim().toLowerCase();
  return sessions.filter((session) => {
    if (filter.status !== 'all' && session.status !== filter.status) return false;
    if (filter.harness !== 'all' && (session.harness ?? '') !== filter.harness) return false;
    if (!q) return true;
    return [
      session.title,
      session.displayLabel,
      session.sessionSummary,
      session.sessionSlug,
      session.id,
      session.harness,
      session.source,
      session.inferenceProvider,
    ].some((field) => field?.toLowerCase().includes(q));
  });
}

export const filteredSessionsStore = derived(
  [sessionsStore, sessionsFilterStore],
  ([$sessions, $filter]) => {
    const filtered = filterSessions($sessions, $filter);
    return [...filtered].sort((a, b) => b.endedAt - a.endedAt);
  },
);

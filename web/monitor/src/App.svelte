<script lang="ts">
  import { get } from 'svelte/store';
  import { onMount } from 'svelte';
  import AnalyticsPanel from './lib/components/AnalyticsPanel.svelte';
  import StatusStrip from './lib/components/StatusStrip.svelte';
  import SessionsPanel from './lib/components/sessions/SessionsPanel.svelte';
  import TopBar from './lib/components/TopBar.svelte';
  import { fetchAnalytics, fetchHealth, fetchStatus } from './lib/api/client';
  import { fetchSessions, searchSessions } from './lib/api/sessions';
  import { FAST_POLL_INTERVAL_MS, SLOW_POLL_INTERVAL_MS } from './lib/constants';
  import { analyticsRangeStore } from './lib/state/monitor';
  import {
    failedSessionSearch,
    filteredSessionsStore,
    idleSessionSearch,
    sessionSearchStore,
    sessionsCursorStore,
    sessionsErrorStore,
    sessionsFilterStore,
    sessionsHasMoreStore,
    sessionsLoadingMoreStore,
    sessionsLoadingStore,
    sessionsStore,
  } from './lib/state/sessions';
  import { initializeTheme, setTheme, themeStore } from './lib/state/theme';
  import type {
    AnalyticsRangeKey,
    AnalyticsResponse,
    HealthResponse,
    StatusResponse,
  } from './lib/types/api';
  import type { SessionsFilter } from './lib/types/sessions';
  import type { ThemeMode } from './lib/types/ui';

  const SESSIONS_POLL_INTERVAL_MS = 30_000;
  const SESSIONS_PAGE_SIZE = 50;
  /**
   * Keystroke settle before a search reaches the backend. Every search is a
   * real BM25 ranking over the whole corpus, so typing must not issue one per
   * character.
   */
  const SEARCH_DEBOUNCE_MS = 250;
  const SEARCH_RESULT_LIMIT = 25;

  /** Whether the reader has paged past page 1. See `refreshSessions`. */
  let sessionsPaged = false;

  let healthData: HealthResponse | null = null;
  let healthError: string | null = null;

  let statusData: StatusResponse | null = null;
  let statusError: string | null = null;

  let analyticsPayload: AnalyticsResponse | null = null;
  let analyticsError: string | null = null;

  $: sessions = $sessionsStore;
  $: filteredSessions = $filteredSessionsStore;
  $: sessionsFilter = $sessionsFilterStore;
  $: sessionsLoading = $sessionsLoadingStore;
  $: sessionsLoadingMore = $sessionsLoadingMoreStore;
  $: sessionsHasMore = $sessionsHasMoreStore;
  $: sessionsError = $sessionsErrorStore;
  $: sessionSearch = $sessionSearchStore;

  // The harness vocabulary is served, not scraped from the loaded page: with a
  // paged feed, options derived from loaded rows would omit every harness whose
  // sessions sit on a page nobody has fetched.
  $: sessionHarnesses = statusData?.known_harnesses ?? [];

  function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }

  async function loadHealth(): Promise<void> {
    try {
      healthData = await fetchHealth();
      healthError = null;
    } catch (error) {
      healthError = errorMessage(error);
      healthData = null;
    }
  }

  async function loadStatus(): Promise<void> {
    try {
      statusData = await fetchStatus();
      statusError = null;
    } catch (error) {
      statusError = errorMessage(error);
      statusData = null;
    }
  }

  async function loadAnalytics(): Promise<void> {
    try {
      analyticsPayload = await fetchAnalytics(get(analyticsRangeStore));
      analyticsError = null;
    } catch (error) {
      analyticsError = `Analytics unavailable: ${errorMessage(error)}`;
    }
  }

  /** Bumped by every feed read so a late response cannot overwrite a newer one. */
  let sessionsRequestGeneration = 0;

  /** Read page 1 under the current server-side filters, discarding any paging. */
  async function loadSessions(): Promise<void> {
    const filter = get(sessionsFilterStore);
    sessionsRequestGeneration += 1;
    sessionsLoadingStore.set(true);
    try {
      const page = await fetchSessions({
        limit: SESSIONS_PAGE_SIZE,
        harness: filter.harness === 'all' ? null : filter.harness,
      });
      sessionsStore.set(page.sessions);
      sessionsCursorStore.set(page.nextCursor);
      sessionsHasMoreStore.set(page.hasMore);
      sessionsErrorStore.set(null);
      sessionsPaged = false;
    } catch (error) {
      // No fabricated sessions on failure: an unreachable backend must not be
      // indistinguishable from an idle one.
      sessionsErrorStore.set(`Sessions unavailable: ${errorMessage(error)}`);
    } finally {
      sessionsLoadingStore.set(false);
    }
  }

  async function loadMoreSessions(): Promise<void> {
    const cursor = get(sessionsCursorStore);
    if (!cursor || get(sessionsLoadingMoreStore)) return;

    // The cursor pins only the time window. `harness`/`source`/`mode`/`sort`
    // are NOT carried in it — the backend refuses a cursor presented under
    // different ones (docs/monitor-http-api.md) — so the continuation must
    // repeat the filters page 1 was read under. Sending the cursor alone
    // makes "load more" fail for every non-`all` harness.
    const filter = get(sessionsFilterStore);
    const generation = ++sessionsRequestGeneration;

    sessionsLoadingMoreStore.set(true);
    try {
      const page = await fetchSessions({
        limit: SESSIONS_PAGE_SIZE,
        cursor,
        harness: filter.harness === 'all' ? null : filter.harness,
      });
      // A filter change while this was in flight already reloaded page 1;
      // appending a superseded-filter page over it would show rows the
      // current filter excludes.
      if (generation !== sessionsRequestGeneration) return;
      sessionsStore.update((loaded) => {
        // A moving feed can hand back a session already loaded (a #602
        // generation replay moves `updated_at` in either direction across
        // requests); rendering it twice is the visible symptom.
        const seen = new Set(loaded.map((session) => session.id));
        return [...loaded, ...page.sessions.filter((session) => !seen.has(session.id))];
      });
      sessionsCursorStore.set(page.nextCursor);
      sessionsHasMoreStore.set(page.hasMore);
      sessionsErrorStore.set(null);
      sessionsPaged = true;
    } catch (error) {
      sessionsErrorStore.set(`Could not load more sessions: ${errorMessage(error)}`);
    } finally {
      sessionsLoadingMoreStore.set(false);
    }
  }

  /**
   * The background refresh only runs while the feed is still on page 1.
   * Re-reading page 1 resets the cursor, so refreshing a paged feed would
   * silently discard the pages the reader had loaded. Leaving it alone is the
   * honest choice; "Load more" still reflects the server's `has_more`.
   */
  async function refreshSessions(): Promise<void> {
    if (sessionsPaged || get(sessionsLoadingMoreStore)) return;
    await loadSessions();
  }

  async function hydrateFast(): Promise<void> {
    await Promise.all([loadHealth(), loadStatus()]);
  }

  async function hydrateSlow(): Promise<void> {
    await Promise.all([loadAnalytics(), loadSessions()]);
  }

  async function handleRangeChange(event: CustomEvent<AnalyticsRangeKey>): Promise<void> {
    analyticsRangeStore.set(event.detail);
    await loadAnalytics();
  }

  function handleSetTheme(event: CustomEvent<ThemeMode>): void {
    setTheme(event.detail);
  }

  /**
   * Whole-corpus search (issue-599 WI-09).
   *
   * The search input is no longer a filter over the loaded page — it is a
   * server-side ranking over every session this backend may serve. A blank
   * query clears the results back to `null`, which is what returns the panel to
   * the time-ordered feed; setting `[]` there would render as "nothing matched".
   */
  let searchTimer: ReturnType<typeof setTimeout> | null = null;
  /** Bumped by every search so a late response cannot overwrite a newer one. */
  let searchGeneration = 0;

  function clearSearch(): void {
    if (searchTimer !== null) {
      clearTimeout(searchTimer);
      searchTimer = null;
    }
    searchGeneration += 1;
    sessionSearchStore.set({ ...idleSessionSearch });
  }

  async function runSearch(query: string): Promise<void> {
    const generation = ++searchGeneration;
    const filter = get(sessionsFilterStore);
    sessionSearchStore.update((state) => ({
      ...state,
      query,
      loading: true,
      error: null,
      errorKind: null,
    }));
    try {
      const page = await searchSessions(query, {
        limit: SEARCH_RESULT_LIMIT,
        harness: filter.harness === 'all' ? null : filter.harness,
      });
      if (generation !== searchGeneration) return;
      sessionSearchStore.set({
        query,
        results: page.sessions,
        loading: false,
        error: null,
        errorKind: null,
        truncated: page.truncated,
        hitsTruncated: page.hitsTruncated,
        incomplete: page.incomplete,
        dropped: page.dropped,
      });
    } catch (error) {
      if (generation !== searchGeneration) return;
      // No fallback to a local filter: answering a failed search with a
      // page-local title match would report a subset of one page as the
      // corpus-wide answer. `failedSessionSearch` owns what a failure looks
      // like — in particular that `results` stays `null` — so the rule is
      // testable rather than living in this `catch`.
      sessionSearchStore.set(failedSessionSearch(query, error));
    }
  }

  function scheduleSearch(query: string): void {
    if (searchTimer !== null) {
      clearTimeout(searchTimer);
    }
    searchTimer = setTimeout(() => {
      searchTimer = null;
      void runSearch(query);
    }, SEARCH_DEBOUNCE_MS);
  }

  function handleFilterChange(event: CustomEvent<SessionsFilter>): void {
    const previous = get(sessionsFilterStore);
    const next = event.detail;
    sessionsFilterStore.set(next);

    const query = next.query.trim();
    const harnessChanged = previous.harness !== next.harness;
    // `harness` narrows the query the server runs, so it cannot be answered
    // from the loaded pages alone: changing it restarts the feed from page 1.
    // `status` is the only control still answered locally.
    if (harnessChanged) {
      void loadSessions();
    }
    if (query === '') {
      clearSearch();
    } else if (previous.query.trim() !== query || harnessChanged) {
      // A harness change re-runs the active search too: the server applies it,
      // so the previous results answer a different question.
      scheduleSearch(query);
    }
  }

  onMount(() => {
    initializeTheme();

    void hydrateFast();
    void hydrateSlow();

    const fastInterval = window.setInterval(() => {
      void hydrateFast();
    }, FAST_POLL_INTERVAL_MS);

    const slowInterval = window.setInterval(() => {
      void loadAnalytics();
    }, SLOW_POLL_INTERVAL_MS);

    const sessionsInterval = window.setInterval(() => {
      void refreshSessions();
    }, SESSIONS_POLL_INTERVAL_MS);

    return () => {
      window.clearInterval(fastInterval);
      window.clearInterval(slowInterval);
      window.clearInterval(sessionsInterval);
      if (searchTimer !== null) {
        clearTimeout(searchTimer);
        searchTimer = null;
      }
    };
  });
</script>

<div class="app-shell">
  <TopBar theme={$themeStore} on:setTheme={handleSetTheme} />

  <main class="layout">
    <StatusStrip health={healthData} {healthError} status={statusData} {statusError} />

    <AnalyticsPanel
      payload={analyticsPayload}
      selectedRange={$analyticsRangeStore}
      errorMessage={analyticsError}
      theme={$themeStore}
      on:rangeChange={handleRangeChange}
    />

    <SessionsPanel
      sessions={sessions}
      filtered={filteredSessions}
      filter={sessionsFilter}
      harnesses={sessionHarnesses}
      loading={sessionsLoading}
      loadingMore={sessionsLoadingMore}
      hasMore={sessionsHasMore}
      errorMessage={sessionsError}
      search={sessionSearch}
      on:filterChange={handleFilterChange}
      on:loadMore={loadMoreSessions}
    />
  </main>
</div>

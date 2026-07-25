<script lang="ts">
  import { get } from 'svelte/store';
  import { onMount } from 'svelte';
  import AnalyticsPanel from './lib/components/AnalyticsPanel.svelte';
  import StatusStrip from './lib/components/StatusStrip.svelte';
  import SessionsPanel from './lib/components/sessions/SessionsPanel.svelte';
  import TopBar from './lib/components/TopBar.svelte';
  import { fetchAnalytics, fetchHealth, fetchStatus } from './lib/api/client';
  import { fetchSessions } from './lib/api/sessions';
  import { FAST_POLL_INTERVAL_MS, SLOW_POLL_INTERVAL_MS } from './lib/constants';
  import { analyticsRangeStore } from './lib/state/monitor';
  import {
    filteredSessionsStore,
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

  function handleFilterChange(event: CustomEvent<SessionsFilter>): void {
    const previous = get(sessionsFilterStore);
    sessionsFilterStore.set(event.detail);
    // `harness` narrows the query the server runs, so it cannot be answered
    // from the loaded pages alone: changing it restarts the feed from page 1.
    // `status` and `query` are page-local and need no round trip.
    if (previous.harness !== event.detail.harness) {
      void loadSessions();
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
      on:filterChange={handleFilterChange}
      on:loadMore={loadMoreSessions}
    />
  </main>
</div>

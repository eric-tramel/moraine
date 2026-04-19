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
    sessionsErrorStore,
    sessionsFilterStore,
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
  import type { Harness, Session, SessionsFilter } from './lib/types/sessions';
  import type { ThemeMode } from './lib/types/ui';

  const SESSIONS_POLL_INTERVAL_MS = 30_000;

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
  $: sessionsError = $sessionsErrorStore;

  $: sessionModels = deriveModels(sessions);
  $: sessionHarnesses = deriveHarnesses(sessions);

  function deriveModels(list: Session[]): string[] {
    const set = new Set<string>();
    for (const s of list) {
      for (const m of s.models) set.add(m);
    }
    return [...set].sort();
  }

  function deriveHarnesses(list: Session[]): Harness[] {
    const map = new Map<string, Harness>();
    for (const s of list) {
      if (!map.has(s.harness.id)) map.set(s.harness.id, s.harness);
    }
    return [...map.values()].sort((a, b) => a.label.localeCompare(b.label));
  }

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

  async function loadSessions(): Promise<void> {
    sessionsLoadingStore.set(true);
    try {
      const list = await fetchSessions();
      sessionsStore.set(list);
      sessionsErrorStore.set(null);
    } catch (error) {
      sessionsErrorStore.set(`Sessions unavailable: ${errorMessage(error)}`);
    } finally {
      sessionsLoadingStore.set(false);
    }
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
    sessionsFilterStore.set(event.detail);
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
      void loadSessions();
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
      models={sessionModels}
      harnesses={sessionHarnesses}
      loading={sessionsLoading}
      errorMessage={sessionsError}
      on:filterChange={handleFilterChange}
    />
  </main>
</div>

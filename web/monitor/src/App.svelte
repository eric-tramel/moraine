<script lang="ts">
  import { get } from 'svelte/store';
  import { onMount } from 'svelte';
  import AnalyticsPanel from './lib/components/AnalyticsPanel.svelte';
  import StatGrid from './lib/components/StatGrid.svelte';
  import TablePanel from './lib/components/TablePanel.svelte';
  import TopBar from './lib/components/TopBar.svelte';
  import { fetchAnalytics, fetchHealth, fetchStatus, fetchTableDetail, fetchTables } from './lib/api/client';
  import { FAST_POLL_INTERVAL_MS, ROW_LIMIT_OPTIONS, SLOW_POLL_INTERVAL_MS } from './lib/constants';
  import { analyticsRangeStore, rowLimitStore, selectedTableStore } from './lib/state/monitor';
  import { initializeTheme, themeStore, toggleTheme } from './lib/state/theme';
  import type { AnalyticsRangeKey, AnalyticsResponse, TableDetailResponse, TableSummary } from './lib/types/api';
  import type { StatCard, TableOption } from './lib/types/ui';
  import { buildHealthCards, buildIngestorCards } from './lib/view/cards';

  let healthCards: StatCard[] = [];
  let ingestorCards: StatCard[] = [];

  let tableOptions: TableOption[] = [];
  let tableDetail: TableDetailResponse | null = null;
  let tableTitle = 'Table';
  let schemaText = 'No schema metadata returned';
  let schemaMuted = true;

  let analyticsPayload: AnalyticsResponse | null = null;
  let analyticsError: string | null = null;

  function errorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }
    return String(error);
  }

  function toTableOptions(tables: TableSummary[]): TableOption[] {
    return [
      ...tables.map((entry) => ({
        value: entry.name,
        label: `${entry.name} (${Number(entry.rows || 0).toLocaleString()} rows)`,
      })),
      { value: 'web_searches', label: 'web_searches (virtual)' },
    ];
  }

  function sectionErrorCards(label: string, message: string): StatCard[] {
    return [
      { label, value: 'Unavailable', tone: 'bad' },
      { label: 'Error', value: message, tone: 'warn' },
    ];
  }

  function updateSchema(detail: TableDetailResponse): void {
    if (!detail.schema || detail.schema.length === 0) {
      schemaText = 'No schema metadata returned';
      schemaMuted = true;
      return;
    }

    schemaText = detail.schema.map((column) => `${column.name}: ${column.type}`).join(' • ');
    schemaMuted = false;
  }

  async function loadHealth(): Promise<void> {
    const data = await fetchHealth();
    healthCards = buildHealthCards(data);
  }

  async function loadStatus(): Promise<void> {
    const data = await fetchStatus();
    ingestorCards = buildIngestorCards(data);
  }

  async function loadTable(tableName: string): Promise<void> {
    tableTitle = `Table: ${tableName}`;
    const detail = await fetchTableDetail(tableName, get(rowLimitStore));
    tableDetail = detail;
    updateSchema(detail);
  }

  async function loadTablesAndSelection(): Promise<void> {
    const data = await fetchTables();
    tableOptions = toTableOptions(data.tables || []);

    const current = get(selectedTableStore);
    if (!current || !tableOptions.some((option) => option.value === current)) {
      selectedTableStore.set(tableOptions.length > 0 ? tableOptions[0].value : null);
    }

    const active = get(selectedTableStore);
    if (!active) {
      tableTitle = 'Table';
      tableDetail = null;
      schemaText = 'No schema metadata returned';
      schemaMuted = true;
      return;
    }

    await loadTable(active);
  }

  async function loadAnalytics(): Promise<void> {
    analyticsPayload = await fetchAnalytics(get(analyticsRangeStore));
    analyticsError = null;
  }

  async function hydrateFast(): Promise<void> {
    const [healthResult, statusResult] = await Promise.allSettled([loadHealth(), loadStatus()]);

    if (healthResult.status === 'rejected') {
      healthCards = sectionErrorCards('ClickHouse', errorMessage(healthResult.reason));
    }

    if (statusResult.status === 'rejected') {
      ingestorCards = sectionErrorCards('Ingestor', errorMessage(statusResult.reason));
    }
  }

  async function hydrateSlow(): Promise<void> {
    const [tableResult, analyticsResult] = await Promise.allSettled([loadTablesAndSelection(), loadAnalytics()]);

    if (tableResult.status === 'rejected') {
      tableTitle = 'Connection issue';
      tableDetail = null;
      schemaText = errorMessage(tableResult.reason);
      schemaMuted = false;
    }

    if (analyticsResult.status === 'rejected') {
      analyticsError = `Analytics unavailable: ${errorMessage(analyticsResult.reason)}`;
    }
  }

  async function hydrateAll(): Promise<void> {
    await Promise.all([hydrateFast(), hydrateSlow()]);
  }

  async function handleRangeChange(event: CustomEvent<AnalyticsRangeKey>): Promise<void> {
    analyticsRangeStore.set(event.detail);
    try {
      await loadAnalytics();
    } catch (error) {
      analyticsError = `Analytics unavailable: ${errorMessage(error)}`;
    }
  }

  async function handleTableChange(event: CustomEvent<string>): Promise<void> {
    selectedTableStore.set(event.detail);

    try {
      await loadTable(event.detail);
    } catch (error) {
      tableDetail = null;
      schemaText = errorMessage(error);
      schemaMuted = false;
    }
  }

  async function handleRowLimitChange(event: CustomEvent<number>): Promise<void> {
    rowLimitStore.set(event.detail);

    const active = get(selectedTableStore);
    if (!active) {
      return;
    }

    try {
      await loadTable(active);
    } catch (error) {
      tableDetail = null;
      schemaText = errorMessage(error);
      schemaMuted = false;
    }
  }

  function handleThemeToggle(): void {
    toggleTheme();
  }

  onMount(() => {
    initializeTheme();
    void hydrateAll();

    const fastInterval = window.setInterval(() => {
      void hydrateFast();
    }, FAST_POLL_INTERVAL_MS);

    const slowInterval = window.setInterval(() => {
      void hydrateSlow();
    }, SLOW_POLL_INTERVAL_MS);

    return () => {
      window.clearInterval(fastInterval);
      window.clearInterval(slowInterval);
    };
  });
</script>

<div class="app-shell">
  <TopBar theme={$themeStore} on:toggleTheme={handleThemeToggle} on:refresh={() => void hydrateAll()} />

  <main class="layout">
    <AnalyticsPanel
      payload={analyticsPayload}
      selectedRange={$analyticsRangeStore}
      errorMessage={analyticsError}
      theme={$themeStore}
      on:rangeChange={handleRangeChange}
    />

    <section class="panel">
      <StatGrid title="Health" cards={healthCards} />
      <StatGrid title="Ingestor Heartbeat" cards={ingestorCards} />
    </section>

    <TablePanel
      tableTitle={tableTitle}
      tableOptions={tableOptions}
      selectedTable={$selectedTableStore}
      rowLimit={$rowLimitStore}
      rowLimitOptions={ROW_LIMIT_OPTIONS}
      schemaText={schemaText}
      schemaMuted={schemaMuted}
      detail={tableDetail}
      on:tableChange={handleTableChange}
      on:rowLimitChange={handleRowLimitChange}
    />
  </main>
</div>

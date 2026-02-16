import { expect, test, type Page } from '@playwright/test';

const codexKeyword = process.env.CORTEX_E2E_CODEX_KEYWORD || '';
const claudeKeyword = process.env.CORTEX_E2E_CLAUDE_KEYWORD || '';
const codexTraceMarker = process.env.CORTEX_E2E_CODEX_TRACE_MARKER || '';
const claudeTraceMarker = process.env.CORTEX_E2E_CLAUDE_TRACE_MARKER || '';

test.skip(!process.env.MONITOR_BASE_URL, 'MONITOR_BASE_URL must be set for the live monitor e2e test');

interface ApiBaseResponse {
  ok: boolean;
}

interface HealthResponse extends ApiBaseResponse {
  connections?: {
    total?: number | null;
  };
}

interface StatusResponse extends ApiBaseResponse {
  ingestor?: {
    latest?: {
      queue_depth?: number | null;
      files_active?: number | null;
      files_watched?: number | null;
    } | null;
  };
}

interface TableSummary {
  name: string;
  rows: number;
}

interface TablesResponse extends ApiBaseResponse {
  tables: TableSummary[];
}

interface TableDetailResponse extends ApiBaseResponse {
  rows: Array<Record<string, unknown>>;
}

interface AnalyticsModelPoint {
  model: string;
}

interface AnalyticsResponse extends ApiBaseResponse {
  series: {
    tokens: AnalyticsModelPoint[];
    turns: AnalyticsModelPoint[];
  };
}

async function getJson<T extends ApiBaseResponse>(page: Page, path: string): Promise<T> {
  const response = await page.request.get(path);
  expect(response.ok(), `request failed for ${path}`).toBeTruthy();
  return (await response.json()) as T;
}

function parseRowsFromTableOption(optionText: string | null): number | null {
  const match = optionText?.match(/\(([^)]+)\s+rows\)/i);
  if (!match) {
    return null;
  }

  const digitsOnly = match[1].replace(/[^\d]/g, '');
  if (!digitsOnly) {
    return null;
  }

  return Number.parseInt(digitsOnly, 10);
}

function parseConnectionTotalFromCard(cardText: string | null): number | null {
  const match = cardText?.match(/Connections\s+([\d,]+)\s+total/i);
  if (!match) {
    return null;
  }

  const digitsOnly = match[1].replace(/[^\d]/g, '');
  if (!digitsOnly) {
    return null;
  }

  return Number.parseInt(digitsOnly, 10);
}

function modelCountFromAnalytics(response: AnalyticsResponse): number {
  const models = new Set<string>();

  for (const point of response.series.tokens || []) {
    models.add(point.model || 'unknown');
  }

  for (const point of response.series.turns || []) {
    models.add(point.model || 'unknown');
  }

  return models.size;
}

test('live monitor UI reflects ingested fixture data', async ({ page }) => {
  const health = await getJson<HealthResponse>(page, '/api/health');
  const status = await getJson<StatusResponse>(page, '/api/status');
  const tables = await getJson<TablesResponse>(page, '/api/tables');
  const analytics = await getJson<AnalyticsResponse>(page, '/api/analytics?range=24h');
  const eventsPreview25 = await getJson<TableDetailResponse>(page, '/api/tables/events?limit=25');

  expect(health.ok).toBe(true);
  expect(status.ok).toBe(true);
  expect(tables.ok).toBe(true);
  expect(analytics.ok).toBe(true);
  expect(eventsPreview25.ok).toBe(true);

  const eventsTable = tables.tables.find((entry) => entry.name === 'events');
  expect(eventsTable).toBeTruthy();

  const expectedEventsRows = Number(eventsTable!.rows || 0);
  expect(expectedEventsRows).toBeGreaterThan(0);

  const expectedModelCount = modelCountFromAnalytics(analytics);
  expect(expectedModelCount).toBeGreaterThan(0);
  const expectedModelText = `${expectedModelCount} model${expectedModelCount === 1 ? '' : 's'}`;

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Cortex Monitor' })).toBeVisible();
  await expect(page.locator('#healthCard .card')).toHaveCount(5);
  await expect(page.locator('#ingestorCard .card').first()).toContainText('Healthy');

  const connectionsCard = page.locator('#healthCard .card').filter({ hasText: 'Connections' });
  await expect(connectionsCard).toContainText('Connections');
  const connectionsTotal = health.connections?.total;
  if (connectionsTotal !== null && connectionsTotal !== undefined) {
    const connectionsCardText = await connectionsCard.textContent();
    const displayedConnections = parseConnectionTotalFromCard(connectionsCardText);
    expect(displayedConnections).not.toBeNull();
    expect(displayedConnections!).toBeGreaterThan(0);
  }

  const latestIngestor = status.ingestor?.latest;
  if (latestIngestor?.queue_depth !== null && latestIngestor?.queue_depth !== undefined) {
    await expect(page.locator('#ingestorCard .card').filter({ hasText: 'Queue depth' })).toContainText(
      String(latestIngestor.queue_depth),
    );
  }
  if (latestIngestor?.files_active !== null && latestIngestor?.files_active !== undefined) {
    await expect(page.locator('#ingestorCard .card').filter({ hasText: 'Files' })).toContainText(
      `${latestIngestor.files_active}`,
    );
  }

  const eventsOption = page.locator('#tableSelect option[value="events"]');
  await expect(eventsOption).toContainText('events');
  const eventsOptionText = await eventsOption.textContent();
  const displayedEventRowCount = parseRowsFromTableOption(eventsOptionText);
  expect(displayedEventRowCount).toBe(expectedEventsRows);

  await page.selectOption('#tableSelect', 'events');
  await expect(page.locator('#tableTitle')).toContainText('Table: events');

  if (codexKeyword) {
    await expect(page.locator('#previewBody')).toContainText(codexKeyword);
  }
  if (claudeKeyword) {
    await expect(page.locator('#previewBody')).toContainText(claudeKeyword);
  }
  if (codexTraceMarker) {
    await expect(page.locator('#previewBody')).toContainText(codexTraceMarker);
  }
  if (claudeTraceMarker) {
    await expect(page.locator('#previewBody')).toContainText(claudeTraceMarker);
  }

  const expectedRowsWithLimit25 = eventsPreview25.rows.length === 0 ? 1 : eventsPreview25.rows.length;
  await expect(page.locator('#previewBody tr')).toHaveCount(expectedRowsWithLimit25);

  await page.selectOption('#rowLimit', '10');
  await expect(page.locator('#rowLimit')).toHaveValue('10');

  const eventsPreview10 = await getJson<TableDetailResponse>(page, '/api/tables/events?limit=10');
  const expectedRowsWithLimit10 = eventsPreview10.rows.length === 0 ? 1 : eventsPreview10.rows.length;
  await expect(page.locator('#previewBody tr')).toHaveCount(expectedRowsWithLimit10);

  await expect(page.locator('#analyticsMeta')).toContainText('Last 24h');
  await expect(page.locator('#analyticsMeta')).toContainText(expectedModelText);

  await page.getByRole('button', { name: '7d' }).click();
  await expect(page.locator('#analyticsMeta')).toContainText('Last 7d');
  await expect(page.locator('#tokensChart')).toBeVisible();
  await expect(page.locator('#turnsChart')).toBeVisible();
  await expect(page.locator('#concurrentSessionsChart')).toBeVisible();
});

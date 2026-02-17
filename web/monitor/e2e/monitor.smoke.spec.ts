import { expect, test } from '@playwright/test';

function analyticsFixture(range: string) {
  const rangeMap: Record<string, { label: string; bucket_seconds: number }> = {
    '15m': { label: 'Last 15m', bucket_seconds: 60 },
    '1h': { label: 'Last 1h', bucket_seconds: 300 },
    '6h': { label: 'Last 6h', bucket_seconds: 900 },
    '24h': { label: 'Last 24h', bucket_seconds: 3600 },
    '7d': { label: 'Last 7d', bucket_seconds: 21600 },
    '30d': { label: 'Last 30d', bucket_seconds: 86400 },
  };

  const picked = rangeMap[range] || rangeMap['24h'];

  return {
    ok: true,
    range: {
      key: range,
      label: picked.label,
      window_seconds: 3600,
      bucket_seconds: picked.bucket_seconds,
      from_unix: 1_700_000_000,
      to_unix: 1_700_003_600,
    },
    series: {
      tokens: [
        { bucket_unix: 1_700_000_000, model: 'gpt-5.3-codex-xhigh', tokens: 1200 },
        { bucket_unix: 1_700_003_600, model: 'gpt-5.3-codex-xhigh', tokens: 900 },
      ],
      turns: [
        { bucket_unix: 1_700_000_000, model: 'gpt-5.3-codex-xhigh', turns: 9 },
        { bucket_unix: 1_700_003_600, model: 'gpt-5.3-codex-xhigh', turns: 8 },
      ],
      concurrent_sessions: [
        { bucket_unix: 1_700_000_000, concurrent_sessions: 3 },
        { bucket_unix: 1_700_003_600, concurrent_sessions: 4 },
      ],
    },
  };
}

test('loads dashboard and handles core interactions', async ({ page }) => {
  await page.route('**/api/health', async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        url: 'http://127.0.0.1:8123',
        database: 'moraine',
        version: '25.1.2',
        ping_ms: 8.75,
        connections: { total: 16 },
      },
    });
  });

  await page.route('**/api/status', async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        ingestor: {
          present: true,
          alive: true,
          age_seconds: 3,
          latest: {
            queue_depth: 1,
            files_active: 2,
            files_watched: 8,
          },
        },
      },
    });
  });

  await page.route('**/api/tables', async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        tables: [
          { name: 'events', engine: 'MergeTree', is_temporary: 0, rows: 1234 },
          { name: 'ingest_heartbeats', engine: 'MergeTree', is_temporary: 0, rows: 15 },
        ],
      },
    });
  });

  await page.route('**/api/tables/events?limit=*', async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        table: 'events',
        limit: 25,
        schema: [
          { name: 'event_ts', type: 'DateTime', default_expression: '' },
          { name: 'model', type: 'String', default_expression: '' },
        ],
        rows: [{ event_ts: '2026-02-16 01:02:03', model: 'gpt-5.3-codex-xhigh' }],
      },
    });
  });

  await page.route('**/api/web-searches?limit=*', async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        table: 'web_searches',
        limit: 25,
        schema: [
          { name: 'search_query', type: 'String', default_expression: '' },
          { name: 'result_url', type: 'String', default_expression: '' },
        ],
        rows: [{ search_query: 'bun typescript vite', result_url: 'https://example.com' }],
      },
    });
  });

  await page.route('**/api/analytics?range=*', async (route) => {
    const requestUrl = new URL(route.request().url());
    const range = requestUrl.searchParams.get('range') || '24h';

    await route.fulfill({
      json: analyticsFixture(range),
    });
  });

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Moraine Monitor' })).toBeVisible();
  await expect(page.locator('#healthCard .card')).toHaveCount(5);
  await expect(page.locator('#ingestorCard .card').first()).toContainText('Healthy');

  await page.selectOption('#tableSelect', 'web_searches');
  await expect(page.locator('#tableTitle')).toContainText('Table: web_searches');
  await expect(page.locator('#previewBody')).toContainText('bun typescript vite');

  await page.selectOption('#rowLimit', '50');
  await expect(page.locator('#rowLimit')).toHaveValue('50');

  await page.getByRole('button', { name: '7d' }).click();
  await expect(page.locator('#analyticsMeta')).toContainText('Last 7d');

  const htmlThemeBefore = await page.locator('html').getAttribute('data-theme');
  await page.locator('#themeToggle').click();
  const htmlThemeAfter = await page.locator('html').getAttribute('data-theme');
  expect(htmlThemeAfter).not.toBe(htmlThemeBefore);
});

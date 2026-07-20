import { expect, test, type Page } from '@playwright/test';

if (!process.env.MONITOR_BASE_URL) {
  throw new Error(
    'MONITOR_BASE_URL is required and must point to the caller-owned monitor stack; the live Playwright suite never provisions one',
  );
}

interface ApiBaseResponse {
  ok: boolean;
}

interface HealthResponse extends ApiBaseResponse {
  url?: string;
  database?: string;
  ping_ms?: number | null;
  connections?: {
    total?: number | null;
  };
  publication?: {
    available: boolean;
    healthy: boolean;
  };
}

interface StatusResponse extends ApiBaseResponse {
  ingestor?: {
    present: boolean;
    alive: boolean;
    latest?: {
      queue_depth?: number | null;
      files_active?: number | null;
      files_watched?: number | null;
    } | null;
  };
}

interface AnalyticsModelPoint {
  model: string;
}

type AnalyticsRangeKey = '15m' | '1h' | '6h' | '24h' | '7d' | '30d';

interface AnalyticsResponse extends ApiBaseResponse {
  range: {
    key: AnalyticsRangeKey;
    label: string;
  };
  series: {
    tokens: AnalyticsModelPoint[];
    turns: AnalyticsModelPoint[];
  };
}

const ANALYTICS_RANGE_CANDIDATES: AnalyticsRangeKey[] = ['24h', '7d', '30d', '6h', '1h', '15m'];

const CANONICAL_API_PATHNAMES = [
  '/api/v1/health',
  '/api/v1/status',
  '/api/v1/analytics',
  '/api/v1/sessions',
] as const;
const STATIC_PATHNAMES = ['/', '/app.js', '/styles.css'] as const;

interface RuntimeTraffic {
  apiPathnames: string[];
  responses: Array<{ origin: string; pathname: string; status: number }>;
}

function trackRuntimeTraffic(page: Page): RuntimeTraffic {
  const traffic: RuntimeTraffic = {
    apiPathnames: [],
    responses: [],
  };

  page.on('request', (request) => {
    const { pathname } = new URL(request.url());
    if (pathname.startsWith('/api/')) {
      traffic.apiPathnames.push(pathname);
    }
  });

  page.on('response', (response) => {
    const { origin, pathname } = new URL(response.url());
    traffic.responses.push({ origin, pathname, status: response.status() });
  });

  return traffic;
}

async function expectVersionedRuntimeTraffic(
  traffic: RuntimeTraffic,
  pageOrigin: string,
): Promise<void> {
  await expect
    .poll(() => [...traffic.apiPathnames], {
      message: 'expected the dashboard to request every canonical monitor endpoint',
    })
    .toEqual(expect.arrayContaining([...CANONICAL_API_PATHNAMES]));

  const legacyApiPathnames = traffic.apiPathnames.filter(
    (pathname) => pathname.startsWith('/api/') && !pathname.startsWith('/api/v1/'),
  );
  expect(legacyApiPathnames).toEqual([]);

  for (const pathname of STATIC_PATHNAMES) {
    await expect
      .poll(
        () =>
          traffic.responses.some(
            (response) =>
              response.origin === pageOrigin &&
              response.pathname === pathname &&
              response.status === 200,
          ),
        { message: `expected same-origin ${pathname} to return 200` },
      )
      .toBe(true);
  }
}

async function getJson<T extends ApiBaseResponse>(page: Page, path: string): Promise<T> {
  const response = await page.request.get(path);
  expect(response.ok(), `request failed for ${path}`).toBeTruthy();
  return (await response.json()) as T;
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

async function findPopulatedAnalytics(
  page: Page,
): Promise<{ analytics: AnalyticsResponse; modelCount: number } | null> {
  for (const range of ANALYTICS_RANGE_CANDIDATES) {
    const analytics = await getJson<AnalyticsResponse>(page, `/api/v1/analytics?range=${range}`);
    expect(analytics.ok).toBe(true);

    const modelCount = modelCountFromAnalytics(analytics);
    if (modelCount > 0) {
      return { analytics, modelCount };
    }
  }

  return null;
}

test('live monitor UI reflects ingested fixture data', async ({ page }) => {
  const health = await getJson<HealthResponse>(page, '/api/v1/health');
  const status = await getJson<StatusResponse>(page, '/api/v1/status');
  const populatedAnalytics = await findPopulatedAnalytics(page);

  expect(health.ok).toBe(true);
  expect(status.ok).toBe(true);
  expect(
    populatedAnalytics,
    'expected at least one populated analytics range among 24h, 7d, 30d, 6h, 1h, 15m',
  ).not.toBeNull();

  const { analytics, modelCount: expectedModelCount } = populatedAnalytics!;
  const expectedModelText = `${expectedModelCount} model${expectedModelCount === 1 ? '' : 's'}`;

  const runtimeTraffic = trackRuntimeTraffic(page);
  const navigationResponse = await page.goto('/');
  expect(navigationResponse).not.toBeNull();
  const pageOrigin = new URL(navigationResponse!.url()).origin;

  await expect(page.getByRole('heading', { name: 'Moraine Monitor' })).toBeVisible();

  const healthGroup = page.locator('#healthGroup');
  await expect(healthGroup).toContainText('ClickHouse');

  if (health.database) {
    await expect(healthGroup).toContainText(health.database);
  }
  if (health.publication?.available) {
    const publicationChip = healthGroup
      .locator('.ss-chip')
      .filter({ hasText: 'publication' })
      .first();
    await expect(publicationChip).toContainText(
      health.publication.healthy ? 'healthy' : 'degraded',
    );
  }

  const ingestorGroup = page.locator('#ingestorGroup');
  if (status.ingestor?.alive) {
    await expect(ingestorGroup).toContainText('healthy');
  }

  await expect(page.locator('#analyticsMeta')).toContainText('Last 24h');

  if (analytics.range.key !== '24h') {
    await page.locator('#analyticsRanges').getByRole('button', { name: analytics.range.key }).click();
  }

  await expect(page.locator('#analyticsMeta')).toContainText(analytics.range.label);
  await expect(page.locator('#analyticsMeta')).toContainText(expectedModelText);

  await expect(page.locator('#tokensChart')).toBeVisible();
  await expect(page.locator('#turnsChart')).toBeVisible();
  await expect(page.locator('#concurrentSessionsChart')).toBeVisible();

  await expect(page.locator('#sessionsPanel')).toBeVisible();

  await expectVersionedRuntimeTraffic(runtimeTraffic, pageOrigin);
});

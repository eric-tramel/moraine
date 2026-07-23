import { expect, test, type Page } from '@playwright/test';

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
        {
          bucket_unix: 1_700_000_000,
          model: 'gpt-5.3-codex-xhigh',
          endpoint_kind: 'generation',
          bucket: 'input_text',
          tokens: 1200,
        },
        {
          bucket_unix: 1_700_003_600,
          model: 'gpt-5.3-codex-xhigh',
          endpoint_kind: 'generation',
          bucket: 'output_text',
          tokens: 900,
        },
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

async function setupMockMonitorApi(page: Page): Promise<void> {
  await page.route('**/api/v1/health', async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        url: 'http://127.0.0.1:8123',
        database: 'moraine',
        version: '25.1.2',
        ping_ms: 8.75,
        connections: { total: 16 },
        publication: {
          available: true,
          healthy: true,
          ambiguous_hostless_rows: 0,
          replaying_generations: 1,
          blocked_generations: 0,
          append_preparations: 0,
          blocked_append_preparations: 0,
          mirror_catchup_pending: 0,
          writer_conflicts: 0,
          issues: ['generation_replaying'],
        },
      },
    });
  });

  await page.route('**/api/v1/status', async (route) => {
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

  await page.route('**/api/v1/sessions', async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        sessions: [
          {
            id: 'session-monitor-fixture',
            title: 'Inspect the repository',
            harness: { id: 'codex', label: 'codex', short: 'C', hue: 150 },
            startedAt: 1_700_000_000_000,
            endedAt: 1_700_000_003_900,
            durationMs: 3_900,
            status: 'completed',
            models: ['gpt-5.3-codex'],
            turns: [
              {
                idx: 0,
                model: 'gpt-5.3-codex',
                startedAt: 1_700_000_000_000,
                endedAt: 1_700_000_003_900,
                durationMs: 3_900,
                promptTokens: 10,
                completionTokens: 6,
                totalTokens: 16,
                toolCalls: 0,
                steps: [
                  { kind: 'user', at: 1_700_000_000_000, text: 'Inspect the repository' },
                  {
                    kind: 'assistant',
                    at: 1_700_000_003_900,
                    text: 'Repository inspection complete',
                    tokens: 6,
                    durationMs: 3_900,
                  },
                ],
              },
            ],
            totalTokens: 16,
            totalToolCalls: 0,
            tags: [],
            traceId: 'trace-monitor-fixture',
          },
        ],
      },
    });
  });

  await page.route('**/api/v1/analytics?range=*', async (route) => {
    const requestUrl = new URL(route.request().url());
    const range = requestUrl.searchParams.get('range') || '24h';

    await route.fulfill({
      json: analyticsFixture(range),
    });
  });
}

async function expectNoPageOverflow(page: Page): Promise<void> {
  const metrics = await page.evaluate(() => {
    const documentWidth = document.documentElement.scrollWidth;
    const bodyWidth = document.body.scrollWidth;
    const viewportWidth = window.innerWidth;

    return {
      viewportWidth,
      documentWidth,
      bodyWidth,
      overflowX: Math.max(documentWidth, bodyWidth) - viewportWidth,
    };
  });

  expect(metrics.overflowX, JSON.stringify(metrics)).toBeLessThanOrEqual(1);
}

test.beforeEach(async ({ page }) => {
  await setupMockMonitorApi(page);
});

test('loads dashboard and handles core interactions', async ({ page }) => {
  const runtimeTraffic = trackRuntimeTraffic(page);
  const navigationResponse = await page.goto('/');
  expect(navigationResponse).not.toBeNull();
  const pageOrigin = new URL(navigationResponse!.url()).origin;

  await expect(page.getByRole('heading', { name: 'Moraine Monitor' })).toBeVisible();

  await expect(page.locator('#healthGroup')).toContainText('ClickHouse');
  await expect(page.locator('#healthGroup')).toContainText('127.0.0.1:8123');
  await expect(page.locator('#healthGroup')).toContainText('moraine');
  const publicationChips = page.locator('#healthGroup .ss-chip').filter({ hasText: 'publication' });
  await expect(publicationChips.first()).toContainText('healthy');
  await expect(publicationChips.last()).toContainText('1 active');
  await expect(page.locator('#ingestorGroup')).toContainText('healthy');

  await page.locator('#analyticsRanges').getByRole('button', { name: '7d' }).click();
  await expect(page.locator('#analyticsMeta')).toContainText('Last 7d');

  // Sessions panel consumes the monitor API response rather than mock fallback data.
  await expect(page.locator('#sessionsPanel')).toBeVisible();
  await expect(page.locator('.mv-card-title').first()).toHaveText('Inspect the repository');
  await expect(page.locator('.mv-card').first()).toBeVisible();

  // Clicking a card opens the side panel with detail (transcript is default)
  await page.locator('.mv-card').first().click();
  await expect(page.locator('.mv-sidepanel')).toBeVisible();
  await expect(page.locator('.mv-nodes')).toBeVisible();

  // Toggle to flamegraph view
  await page.locator('.mv-viz-toggle button', { hasText: 'flamegraph' }).click();
  const flameTurn = page.locator('.mv-flame-turn').first();
  await expect(flameTurn).toBeVisible();
  await expect(flameTurn.locator('.mv-tr-row').first()).toBeVisible();

  // Back to transcript
  await page.locator('.mv-viz-toggle button', { hasText: 'transcript' }).click();
  await expect(page.locator('.mv-nodes')).toBeVisible();

  await page.locator('.mv-sidepanel .mv-iconbtn').click();
  await expect(page.locator('.mv-sidepanel')).toHaveCount(0);

  // Filter bar: searching narrows results
  const counter = page.locator('.mv-filter-count');
  const totalText = (await counter.textContent()) ?? '';
  const totalMatch = totalText.match(/\d+\s*\/\s*(\d+)/);
  expect(totalMatch).not.toBeNull();

  await page.locator('.mv-search-input').fill('nothing-should-match-xyz');
  await expect(page.locator('.mv-empty')).toContainText('No sessions match');
  await page.locator('.mv-search-clear').click();

  // Theme segmented switch
  const htmlThemeBefore = await page.locator('html').getAttribute('data-theme');
  const otherTheme = htmlThemeBefore === 'dark' ? 'light' : 'dark';
  await page.locator(otherTheme === 'dark' ? '#themeDark' : '#themeLight').click();
  const htmlThemeAfter = await page.locator('html').getAttribute('data-theme');
  expect(htmlThemeAfter).toBe(otherTheme);

  await expectVersionedRuntimeTraffic(runtimeTraffic, pageOrigin);
});

test('keeps dashboard and detail views inside the mobile viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Moraine Monitor' })).toBeVisible();
  await expect(page.locator('#sessionsPanel')).toBeVisible();
  await expectNoPageOverflow(page);

  await page.locator('.mv-card').first().click();
  await expect(page.locator('.mv-sidepanel')).toBeVisible();
  await expectNoPageOverflow(page);

  await page.locator('.mv-viz-toggle button', { hasText: 'flamegraph' }).click();
  await expect(page.locator('.mv-flame-turn').first()).toBeVisible();
  await expect(page.locator('.mv-tr-row').first()).toBeVisible();
  await expectNoPageOverflow(page);
});

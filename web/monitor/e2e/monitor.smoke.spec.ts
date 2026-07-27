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

const SESSION_FIXTURE_ID = 'session-monitor-fixture';
const SESSIONS_PAGE_TWO_CURSOR = 'cursor-page-2';
const SESSION_PAGE_PATHNAME = `/api/v1/sessions/${SESSION_FIXTURE_ID}/page`;
const SESSION_SEARCH_PATHNAME = '/api/v1/sessions/search';
/**
 * A session the FEED never returns, whose title shares no substring with the
 * search term. It is reachable only because the server ranked message content
 * across the whole corpus — which is exactly what WI-09 replaced the page-local
 * filter with, and what no client-side predicate could produce.
 */
const SEARCH_ONLY_SESSION_ID = 'session-only-findable-by-content';
const SEARCH_TERM = 'projection';

function sessionSummaryFixture(id: string, title: string) {
  return {
    id,
    title,
    displayLabel: title,
    harness: 'codex',
    source: 'ci-codex',
    inferenceProvider: 'openai',
    mode: 'tool_calling',
    startedAt: 1_700_000_000_000,
    endedAt: 1_700_000_003_900,
    status: 'completed',
    turnCount: 1,
    eventCount: 4,
    toolCallCount: 1,
    sessionSlug: title.toLowerCase().replace(/\s+/g, '-'),
    sessionSummary: `${title} summary`,
  };
}

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
        known_harnesses: ['claude-code', 'codex', 'hermes'],
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

  // Summaries only: no `turns` key, no message content. The dashboard renders
  // a card from exactly these fields.
  await page.route('**/api/v1/sessions?*', async (route) => {
    const url = new URL(route.request().url());
    const cursor = url.searchParams.get('cursor');
    if (cursor === SESSIONS_PAGE_TWO_CURSOR) {
      await route.fulfill({
        json: {
          ok: true,
          read_model: 'live',
          sessions: [sessionSummaryFixture('session-monitor-page-2', 'Second page session')],
          limit: 1,
          next_cursor: null,
          has_more: false,
          window: { start: 1_700_000_000_000, end: 1_700_000_100_000 },
        },
      });
      return;
    }
    await route.fulfill({
      json: {
        ok: true,
        read_model: 'live',
        sessions: [sessionSummaryFixture(SESSION_FIXTURE_ID, 'Inspect the repository')],
        limit: 1,
        next_cursor: SESSIONS_PAGE_TWO_CURSOR,
        has_more: true,
        window: { start: 1_700_000_000_000, end: 1_700_000_100_000 },
      },
    });
  });

  // Registered before the `:id/page` route below; Playwright gives priority to
  // the most recently registered matching handler, and this path must never be
  // read as a session whose id is "search".
  await page.route('**/api/v1/sessions/search?*', async (route) => {
    const url = new URL(route.request().url());
    const q = (url.searchParams.get('q') ?? '').trim();
    const matches =
      q === SEARCH_TERM
        ? [
            sessionSummaryFixture(
              SEARCH_ONLY_SESSION_ID,
              'Retire the read-model rebuild',
            ),
          ]
        : [];
    await route.fulfill({
      json: {
        ok: true,
        read_model: 'live',
        query: q,
        terms: q ? q.split(/\s+/) : [],
        sessions: matches,
        limit: 25,
        result_count: matches.length,
        truncated: false,
        hits_truncated: false,
        incomplete: false,
        dropped: false,
      },
    });
  });

  await page.route('**/api/v1/sessions/*/page?*', async (route) => {
    const url = new URL(route.request().url());
    const sessionId = decodeURIComponent(url.pathname.split('/').slice(-2)[0]);
    await route.fulfill({
      json: {
        ok: true,
        read_model: 'live',
        limit: 25,
        session: {
          id: sessionId,
          title: 'Inspect the repository',
          harness: 'codex',
          source: 'ci-codex',
          inferenceProvider: 'openai',
          mode: 'tool_calling',
          startedAt: 1_700_000_000_000,
          endedAt: 1_700_000_003_900,
          completed: true,
          turnCount: 1,
          eventCount: 4,
          sessionSlug: 'inspect-the-repository',
          sessionSummary: 'Repository inspection complete',
          turns: [
            {
              turnSeq: 1,
              turnId: 'turn-1',
              startedAt: 1_700_000_000_000,
              endedAt: 1_700_000_003_900,
              eventCount: 4,
              userMessages: 1,
              assistantMessages: 1,
              toolCalls: 1,
              toolResults: 1,
              reasoningItems: 0,
              userInput: 'Inspect the repository',
              finalResponse: 'Repository inspection complete',
              toolsCalled: ['Read'],
              completed: true,
            },
          ],
        },
        has_more: false,
        next_cursor: null,
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

  // Sessions panel consumes the monitor API response. There is no fallback to
  // generated data, so anything rendered here came from the feed.
  await expect(page.locator('#sessionsPanel')).toBeVisible();
  await expect(page.locator('.mv-card-title').first()).toHaveText('Inspect the repository');
  await expect(page.locator('.mv-card').first()).toBeVisible();

  // The collapsed list renders from summaries alone: no transcript request has
  // been made for any session.
  expect(runtimeTraffic.apiPathnames.filter((p) => p.endsWith('/page'))).toEqual([]);

  // Opening a card lazily loads exactly one page of turns.
  await page.locator('.mv-card').first().click();
  await expect(page.locator('.mv-sidepanel')).toBeVisible();
  await expect(page.locator('.mv-nodes')).toBeVisible();
  await expect(page.locator('.mv-node-user .mv-node-text')).toHaveText('Inspect the repository');
  await expect(page.locator('.mv-node-assistant .mv-node-text')).toHaveText(
    'Repository inspection complete',
  );
  await expect
    .poll(() => runtimeTraffic.apiPathnames.filter((p) => p === SESSION_PAGE_PATHNAME).length)
    .toBe(1);

  await page.locator('.mv-sidepanel .mv-iconbtn').click();
  await expect(page.locator('.mv-sidepanel')).toHaveCount(0);

  // The count says what is loaded, never a ratio against a total the feed does
  // not report.
  const counter = page.locator('.mv-filter-count');
  await expect(counter).toHaveText('1 loaded');

  // `has_more` surfaces as an affordance, so a page can never read as the whole
  // corpus. Redeeming the cursor appends page 2.
  await page.getByRole('button', { name: 'Load more sessions' }).click();
  await expect(page.locator('.mv-card')).toHaveCount(2);
  await expect(counter).toHaveText('2 loaded');
  await expect(page.getByRole('button', { name: 'Load more sessions' })).toHaveCount(0);

  // Filter bar: the input is a whole-corpus server search, and says so.
  await expect(page.locator('.mv-search-input')).toHaveAttribute(
    'placeholder',
    'Search all sessions by content',
  );
  await page.locator('.mv-search-input').fill('nothing-should-match-xyz');
  await expect(page.locator('#sessionsPanel .mv-empty')).toContainText('No sessions match');
  await page.locator('.mv-search-clear').click();
  await expect(counter).toHaveText('2 loaded');

  // Theme segmented switch
  const htmlThemeBefore = await page.locator('html').getAttribute('data-theme');
  const otherTheme = htmlThemeBefore === 'dark' ? 'light' : 'dark';
  await page.locator(otherTheme === 'dark' ? '#themeDark' : '#themeLight').click();
  const htmlThemeAfter = await page.locator('html').getAttribute('data-theme');
  expect(htmlThemeAfter).toBe(otherTheme);

  await expectVersionedRuntimeTraffic(runtimeTraffic, pageOrigin);
});

test('surfaces a failed session feed as an error instead of fabricated sessions', async ({
  page,
}) => {
  // The deleted mock fallback answered every failure with generated sessions
  // and a null error store, which made an unreachable backend look like a live
  // one serving unfamiliar work.
  await page.route('**/api/v1/sessions?*', async (route) => {
    await route.fulfill({
      status: 504,
      json: { ok: false, error: 'sessions query failed: deadline', code: 'deadline_exceeded' },
    });
  });

  await page.goto('/');
  await expect(page.locator('#sessionsPanel')).toBeVisible();
  await expect(page.locator('#sessionsPanel .mv-empty').first()).toContainText(
    'Sessions unavailable',
  );
  await expect(page.locator('.mv-card')).toHaveCount(0);
});

test('keeps dashboard and detail views inside the mobile viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Moraine Monitor' })).toBeVisible();
  await expect(page.locator('#sessionsPanel')).toBeVisible();
  await expectNoPageOverflow(page);

  await page.locator('.mv-card').first().click();
  await expect(page.locator('.mv-sidepanel')).toBeVisible();
  await expect(page.locator('.mv-nodes')).toBeVisible();
  await expectNoPageOverflow(page);
});

test('search reaches the server and opens a result no local filter could find', async ({
  page,
}) => {
  const runtimeTraffic = trackRuntimeTraffic(page);
  await page.goto('/');
  await expect(page.locator('#sessionsPanel')).toBeVisible();
  await expect(page.locator('.mv-card')).toHaveCount(1);

  // The searched term appears in no loaded row's title, id, slug or label. The
  // deleted page-local filter matched exactly those fields, so it could only
  // have produced an empty list here.
  await expect(page.locator('.mv-card-title').first()).toHaveText('Inspect the repository');
  // Typed one character at a time, which is the only input shape a debounce can
  // be observed through: `fill()` emits a single event and would report one
  // request whether or not the debounce exists.
  await page.locator('.mv-search-input').pressSequentially(SEARCH_TERM, { delay: 20 });

  await expect
    .poll(() => runtimeTraffic.apiPathnames.filter((p) => p === SESSION_SEARCH_PATHNAME).length, {
      message: 'expected the query to reach the server-side search route',
    })
    .toBeGreaterThan(0);
  await expect(page.locator('.mv-card-title')).toHaveText(['Retire the read-model rebuild']);
  await expect(page.locator('.mv-filter-count')).toHaveText('1 result');
  // A ranking is bounded, not paged: the feed's continuation affordance must
  // not offer to extend a result set its cursor does not describe.
  await expect(page.getByRole('button', { name: 'Load more sessions' })).toHaveCount(0);

  // The count and the server's bounded-answer flags describe two different
  // populations as soon as a LOCAL filter narrows the ranked set. `status`
  // is client-side, so selecting `active` hides the (completed) result — and
  // the bar must then report both numbers rather than presenting a local
  // narrowing as the ranking's own answer. This is the wiring the pure
  // `sessionCountLabel` unit tests cannot reach.
  const statusSelect = page.locator('.mv-filter select').first();
  await statusSelect.selectOption('active');
  await expect(page.locator('.mv-filter-count')).toHaveText('0 of 1 results');
  await expect(page.locator('.mv-card')).toHaveCount(0);
  await statusSelect.selectOption('all');
  await expect(page.locator('.mv-filter-count')).toHaveText('1 result');

  // Every search is a real BM25 ranking over the whole corpus, so typing must
  // settle before one is issued. The bound is 2, not "fewer than the number of
  // keystrokes": a debounce degraded to one request per keystroke minus one
  // would satisfy the looser bound while issuing nine rankings for ten
  // characters. Two allows for the settle boundary landing mid-word once.
  const searchRequests = runtimeTraffic.apiPathnames.filter(
    (p) => p === SESSION_SEARCH_PATHNAME,
  ).length;
  expect(
    searchRequests,
    `${SEARCH_TERM.length} keystrokes issued ${searchRequests} searches`,
  ).toBeLessThanOrEqual(2);

  // A result is a session summary, so it opens through the same lazy reader a
  // listed session does — no search-specific detail path.
  await page.locator('.mv-card').first().click();
  await expect(page.locator('.mv-sidepanel')).toBeVisible();
  await expect
    .poll(() =>
      runtimeTraffic.apiPathnames.filter(
        (p) => p === `/api/v1/sessions/${SEARCH_ONLY_SESSION_ID}/page`,
      ).length,
    )
    .toBe(1);
  await page.locator('.mv-sidepanel .mv-iconbtn').click();

  // Clearing the query returns the panel to the time-ordered feed rather than
  // leaving an empty result set on screen.
  await page.locator('.mv-search-clear').click();
  await expect(page.locator('.mv-card-title').first()).toHaveText('Inspect the repository');
  await expect(page.locator('.mv-filter-count')).toHaveText('1 loaded');
});

test('a failed search is reported, never answered from the loaded page', async ({ page }) => {
  const runtimeTraffic = trackRuntimeTraffic(page);
  await page.route('**/api/v1/sessions/search?*', async (route) => {
    await route.fulfill({
      status: 504,
      json: { ok: false, error: 'session search failed: deadline', code: 'deadline_exceeded' },
    });
  });

  await page.goto('/');
  await expect(page.locator('#sessionsPanel')).toBeVisible();
  // Typed, not `fill()`ed: a failing search must go through the same debounced
  // path a succeeding one does, or this never exercises the code that runs in
  // production.
  await page.locator('.mv-search-input').pressSequentially('repository', { delay: 20 });
  await expect
    .poll(() =>
      runtimeTraffic.apiPathnames.filter((p) => p === SESSION_SEARCH_PATHNAME).length,
    )
    .toBeGreaterThan(0);

  // "repository" IS in the loaded row's title. A fallback to the deleted local
  // filter would therefore render that row and look like a working search.
  await expect(page.locator('#sessionsPanel .mv-empty').first()).toContainText(
    'Search unavailable',
  );
  await expect(page.locator('.mv-card')).toHaveCount(0);

  // THE POINT OF THE GUARD. The banner alone is also true of the broken
  // behaviour this replaced, which additionally printed "No sessions match this
  // search." and "0 results" — a corpus-wide claim the request never
  // established, beside a banner saying it could not be established.
  const panel = page.locator('#sessionsPanel');
  await expect(panel).not.toContainText('No sessions match');
  await expect(panel).not.toContainText('0 results');
  await expect(page.locator('.mv-filter-count')).toHaveText('search did not run');
  // A ranking's continuation affordance must not appear either: there is
  // nothing to continue.
  await expect(page.getByRole('button', { name: 'Load more sessions' })).toHaveCount(0);

  // Clearing returns the panel to the feed, so the failure is not sticky.
  await page.locator('.mv-search-clear').click();
  await expect(page.locator('.mv-card-title').first()).toHaveText('Inspect the repository');
  await expect(page.locator('.mv-filter-count')).toHaveText('1 loaded');
});

test('a query the server cannot tokenize is a hint, not an outage', async ({ page }) => {
  // The backend owns the tokenizer, so it decides that a one-character or
  // non-ASCII query carries no searchable term. Reported as `503` this rendered
  // "Search unavailable" — an outage banner for a typo, with a retry that could
  // never succeed.
  await page.route('**/api/v1/sessions/search?*', async (route) => {
    await route.fulfill({
      status: 400,
      json: {
        ok: false,
        error:
          'session search failed: query has no searchable terms (tokens shorter than 2 characters are excluded)',
        code: 'invalid_argument',
      },
    });
  });

  await page.goto('/');
  await expect(page.locator('#sessionsPanel')).toBeVisible();
  await page.locator('.mv-search-input').pressSequentially('x', { delay: 20 });

  const panel = page.locator('#sessionsPanel');
  await expect(panel.locator('.mv-empty').first()).toContainText('no searchable terms');
  await expect(panel).not.toContainText('Search unavailable');
  await expect(panel).not.toContainText('No sessions match');
  await expect(panel).not.toContainText('0 results');
});

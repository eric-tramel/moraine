import { afterEach, describe, expect, it, vi } from 'vitest';
import { fetchAnalytics, fetchHealth, fetchStatus } from './client';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('versioned monitor requests', () => {
  it.each([
    {
      label: 'health',
      request: () => fetchHealth(),
      expectedPath: '/api/v1/health',
    },
    {
      label: 'status',
      request: () => fetchStatus(),
      expectedPath: '/api/v1/status',
    },
    {
      label: 'analytics',
      request: () => fetchAnalytics('7d'),
      expectedPath: '/api/v1/analytics?range=7d',
    },
  ])('fetches $label with the canonical URL and JSON header', async ({ request, expectedPath }) => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    );
    vi.stubGlobal('fetch', fetchMock);

    await expect(request()).resolves.toEqual({ ok: true });
    expect(fetchMock).toHaveBeenCalledOnce();
    expect(fetchMock).toHaveBeenCalledWith(expectedPath, {
      headers: { Accept: 'application/json' },
    });
  });
});

describe('request errors', () => {

  it('uses API error text from json error payloads', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ error: 'service unavailable' }), {
          status: 503,
          headers: { 'content-type': 'application/json' },
        }),
      ),
    );

    await expect(fetchHealth()).rejects.toThrow('service unavailable');
  });

  it('falls back to status-based errors when failure payload is non-json', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response('<html>gateway timeout</html>', {
          status: 504,
          headers: { 'content-type': 'text/html' },
        }),
      ),
    );

    await expect(fetchHealth()).rejects.toThrow('request failed (504)');
  });
});

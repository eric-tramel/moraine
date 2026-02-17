import { afterEach, describe, expect, it, vi } from 'vitest';
import { fetchHealth } from './client';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('fetchHealth', () => {
  it('returns parsed json for successful responses', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    );
    vi.stubGlobal('fetch', fetchMock);

    await expect(fetchHealth()).resolves.toEqual({ ok: true });
    expect(fetchMock).toHaveBeenCalledWith('/api/health', {
      headers: { Accept: 'application/json' },
    });
  });

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

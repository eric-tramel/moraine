import { describe, expect, it } from 'vitest';
import { buildAnalyticsView } from './analytics';

const baseRange = {
  key: '24h' as const,
  label: 'Last 24h',
  window_seconds: 24 * 3600,
  bucket_seconds: 3600,
  from_unix: 1_700_000_000,
  to_unix: 1_700_007_200,
};

describe('buildAnalyticsView', () => {
  it('builds chart datasets and metadata', () => {
    const view = buildAnalyticsView({
      ok: true,
      range: baseRange,
      series: {
        tokens: [
          { bucket_unix: 1_700_000_000, model: 'gpt-5.3-codex-xhigh', tokens: 1200 },
          { bucket_unix: 1_700_003_600, model: 'gpt-5.3-codex-xhigh', tokens: 800 },
          { bucket_unix: 1_700_000_000, model: 'haiku', tokens: 100 },
        ],
        turns: [
          { bucket_unix: 1_700_000_000, model: 'gpt-5.3-codex-xhigh', turns: 12 },
          { bucket_unix: 1_700_003_600, model: 'haiku', turns: 3 },
        ],
        concurrent_sessions: [
          { bucket_unix: 1_700_000_000, concurrent_sessions: 4 },
          { bucket_unix: 1_700_003_600, concurrent_sessions: 5 },
        ],
      },
    });

    expect(view.labels.length).toBeGreaterThan(0);
    expect(view.tokenDatasets).toHaveLength(2);
    expect(view.turnDatasets).toHaveLength(2);
    expect(view.concurrentDatasets).toHaveLength(1);
    expect(view.metaText).toContain('Last 24h');
    expect(view.maxTicks).toBe(10);
  });

  it('uses wider tick limit for 30d ranges', () => {
    const view = buildAnalyticsView({
      ok: true,
      range: {
        ...baseRange,
        key: '30d',
        label: 'Last 30d',
        bucket_seconds: 86400,
      },
      series: {
        tokens: [],
        turns: [],
        concurrent_sessions: [],
      },
    });

    expect(view.maxTicks).toBe(12);
    expect(view.metaText).toContain('Last 30d');
  });
});

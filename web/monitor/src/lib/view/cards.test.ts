import { describe, expect, it } from 'vitest';
import { buildHealthCards, buildIngestorCards } from './cards';

describe('buildHealthCards', () => {
  it('maps healthy response fields into cards', () => {
    const cards = buildHealthCards({
      ok: true,
      url: 'http://localhost:8123',
      database: 'cortex',
      version: '25.1.1',
      ping_ms: 12.345,
      connections: { total: 42 },
    });

    expect(cards).toHaveLength(5);
    expect(cards[0]).toMatchObject({ label: 'ClickHouse', value: 'http://localhost:8123', tone: 'ok' });
    expect(cards[3].value).toBe('12.35 ms');
    expect(cards[4].value).toBe('42 total');
  });

  it('maps unhealthy response fields into fallback cards', () => {
    const cards = buildHealthCards({
      ok: false,
      error: 'boom',
      connections: { error: 'db unavailable' },
    });

    expect(cards).toEqual([
      { label: 'ClickHouse', value: 'Unavailable', tone: 'bad' },
      { label: 'Reason', value: 'boom', tone: 'bad' },
      { label: 'Connections', value: 'unavailable', tone: 'warn' },
    ]);
  });
});

describe('buildIngestorCards', () => {
  it('renders healthy ingestor cards', () => {
    const cards = buildIngestorCards({
      ok: true,
      ingestor: {
        present: true,
        alive: true,
        age_seconds: 4,
        latest: {
          queue_depth: 2,
          files_active: 3,
          files_watched: 10,
        },
      },
    });

    expect(cards[0]).toMatchObject({ label: 'Ingestor', value: 'Healthy', tone: 'ok' });
    expect(cards).toContainEqual({ label: 'Heartbeat age', value: '4s', tone: 'ok' });
    expect(cards).toContainEqual({ label: 'Queue depth', value: '2', tone: 'neutral' });
  });

  it('renders missing-ingestor fallback', () => {
    const cards = buildIngestorCards({ ok: true });
    expect(cards).toEqual([{ label: 'Ingestor', value: 'Unknown', tone: 'warn' }]);
  });
});

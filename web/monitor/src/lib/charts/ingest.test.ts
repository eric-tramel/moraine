import { describe, expect, it } from 'vitest';
import type { IngestHistoryPoint, IngestStatus } from '../types/api';
import { buildIngestChartView } from './ingest';

function statusWithHistory(history: IngestHistoryPoint[]): IngestStatus {
  return {
    observed_at_unix_ms: 9_000,
    heartbeat: { table_present: true, latest: null },
    conditions: [],
    alerts: [],
    history,
  };
}

function point(overrides: Partial<IngestHistoryPoint> = {}): IngestHistoryPoint {
  return {
    ts_unix_ms: 1_000,
    queue_depth: 0,
    files_active: 0,
    queue_capacity: 16,
    sink_pending_rows: 0,
    sink_retrying: false,
    discovery_complete: true,
    files_total: 10,
    files_completed: 2,
    bytes_total: 100,
    bytes_completed: 20,
    ...overrides,
  };
}

describe('ingest history charts', () => {
  it('preserves every timestamp and leaves truthful nullable throughput gaps', () => {
    const history = [
      point(),
      point({ ts_unix_ms: 2_000, files_completed: 4, bytes_completed: 40 }),
      point({ ts_unix_ms: 3_000, files_completed: 6, bytes_total: 0, bytes_completed: 0 }),
      point({ ts_unix_ms: 4_000, files_completed: 8, bytes_completed: 10 }),
      point({ ts_unix_ms: 5_000, files_completed: 9, bytes_completed: 5 }),
      point({ ts_unix_ms: 5_000, files_completed: 10, bytes_completed: 6 }),
      point({ ts_unix_ms: 7_000, files_completed: 10, bytes_total: 200, bytes_completed: 20 }),
      point({ ts_unix_ms: 8_000, files_completed: 10, bytes_completed: undefined as unknown as number }),
    ];

    const view = buildIngestChartView(statusWithHistory(history));

    expect(view?.labels).toHaveLength(history.length);
    expect(view?.completionDatasets[0].data).toEqual([20, 40, null, 10, 5, 6, 10, null]);
    expect(view?.completionDatasets[1].data).toEqual([20, 40, 60, 80, 90, 100, 100, 100]);
    expect(view?.throughputDatasets[0].data).toEqual([
      null,
      20,
      null,
      null,
      null,
      null,
      null,
      null,
    ]);
    expect(view?.throughputDatasets[0].spanGaps).toBe(false);
  });

  it('keeps unsupported completion dimensions null instead of claiming completion', () => {
    const view = buildIngestChartView(
      statusWithHistory([
        point({ files_total: 0, files_completed: 0, bytes_total: 100, bytes_completed: 100 }),
      ]),
    );

    expect(view?.completionDatasets[0].data).toEqual([100]);
    expect(view?.completionDatasets[1].data).toEqual([null]);
  });

  it('leaves completion dimensions gapped until discovery freezes the denominator', () => {
    const view = buildIngestChartView(
      statusWithHistory([point({ discovery_complete: false, bytes_completed: 100 })]),
    );

    expect(view?.completionDatasets[0].data).toEqual([null]);
    expect(view?.completionDatasets[1].data).toEqual([null]);
  });

  it('caps client chart samples at the newest 120 points', () => {
    const history = Array.from({ length: 121 }, (_, index) =>
      point({ ts_unix_ms: (index + 1) * 1_000, bytes_total: 200, bytes_completed: index }),
    );

    const view = buildIngestChartView(statusWithHistory(history));

    expect(view?.labels).toHaveLength(120);
    expect(view?.completionDatasets[0].data).toHaveLength(120);
    expect(view?.completionDatasets[0].data[0]).toBe(0.5);
  });
});

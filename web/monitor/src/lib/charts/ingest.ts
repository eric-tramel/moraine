import type { ChartDataset } from 'chart.js';
import type { IngestHistoryPoint, IngestStatus } from '../types/api';
import { chartTheme } from './theme';

export interface IngestChartView {
  labels: string[];
  completionDatasets: ChartDataset<'line', Array<number | null>>[];
  throughputDatasets: ChartDataset<'line', Array<number | null>>[];
}

function labelFor(point: IngestHistoryPoint): string {
  const timestamp = Number(point.ts_unix_ms);
  if (!Number.isFinite(timestamp) || timestamp <= 0) return 'unknown';
  return new Date(timestamp).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function completionPercent(completed: number, total: number): number | null {
  if (
    !Number.isFinite(completed) ||
    !Number.isFinite(total) ||
    total <= 0 ||
    completed < 0 ||
    completed > total
  ) {
    return null;
  }
  return Math.min(100, (completed / total) * 100);
}

function durableByteThroughput(
  previous: IngestHistoryPoint | undefined,
  current: IngestHistoryPoint,
): number | null {
  if (!previous) return null;

  const previousTimestamp = Number(previous.ts_unix_ms);
  const currentTimestamp = Number(current.ts_unix_ms);
  const previousTotal = Number(previous.bytes_total);
  const currentTotal = Number(current.bytes_total);
  const previousCompleted = Number(previous.bytes_completed);
  const currentCompleted = Number(current.bytes_completed);
  if (
    !Number.isFinite(previousTimestamp) ||
    !Number.isFinite(currentTimestamp) ||
    !Number.isFinite(previousTotal) ||
    !Number.isFinite(currentTotal) ||
    !Number.isFinite(previousCompleted) ||
    !Number.isFinite(currentCompleted) ||
    currentTimestamp <= previousTimestamp ||
    previousTotal <= 0 ||
    currentTotal <= 0 ||
    currentTotal !== previousTotal ||
    previousCompleted < 0 ||
    currentCompleted < previousCompleted ||
    currentCompleted > currentTotal
  ) {
    return null;
  }

  return (currentCompleted - previousCompleted) / ((currentTimestamp - previousTimestamp) / 1_000);
}

export function buildIngestChartView(status: IngestStatus): IngestChartView | null {
  const samples = (status.history ?? []).slice(-120);
  if (samples.length === 0) return null;

  const palette = chartTheme();
  return {
    labels: samples.map(labelFor),
    completionDatasets: [
      {
        label: 'Durable byte coverage',
        data: samples.map((point) =>
          point.discovery_complete
            ? completionPercent(Number(point.bytes_completed), Number(point.bytes_total))
            : null,
        ),
        borderColor: palette.good,
        backgroundColor: palette.good,
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.16,
        spanGaps: false,
      },
      {
        label: 'Durable file coverage',
        data: samples.map((point) =>
          point.discovery_complete
            ? completionPercent(Number(point.files_completed), Number(point.files_total))
            : null,
        ),
        borderColor: palette.primary,
        backgroundColor: palette.primary,
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.16,
        spanGaps: false,
      },
    ],
    throughputDatasets: [
      {
        label: 'Durable checkpoint bytes/s',
        data: samples.map((point, index) => durableByteThroughput(samples[index - 1], point)),
        borderColor: palette.warn,
        backgroundColor: palette.warn,
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.12,
        spanGaps: false,
      },
    ],
  };
}

import type { AnalyticsRange, AnalyticsRangeKey } from '../types/api';

export function formatBucketLabel(unixSeconds: number, rangeKey: AnalyticsRangeKey): string {
  const date = new Date(unixSeconds * 1000);

  if (rangeKey === '30d') {
    return date.toLocaleString([], { month: 'short', day: 'numeric' });
  }
  if (rangeKey === '7d' || rangeKey === '24h') {
    return date.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit' });
  }

  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

export function formatBucketSize(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h`;
  return `${Math.round(seconds / 86400)}d`;
}

export function formatCompactNumber(value: number): string {
  const abs = Math.abs(value);

  if (abs >= 1_000_000_000) {
    const decimals = abs >= 10_000_000_000 ? 0 : 1;
    return `${(value / 1_000_000_000).toFixed(decimals).replace(/\.0$/, '')}B`;
  }
  if (abs >= 1_000_000) {
    const decimals = abs >= 10_000_000 ? 0 : 1;
    return `${(value / 1_000_000).toFixed(decimals).replace(/\.0$/, '')}M`;
  }
  if (abs >= 1_000) {
    const decimals = abs >= 10_000 ? 0 : 1;
    return `${(value / 1_000).toFixed(decimals).replace(/\.0$/, '')}K`;
  }

  return `${value}`;
}

export function buildBucketAxis(range: AnalyticsRange): number[] {
  const bucketSeconds = Number(range.bucket_seconds || 3600);
  const fromUnix = Number(range.from_unix || 0);
  const toUnix = Number(range.to_unix || 0);

  if (!fromUnix || !toUnix || bucketSeconds <= 0) return [];

  const start = Math.floor(fromUnix / bucketSeconds) * bucketSeconds;
  const end = Math.floor(toUnix / bucketSeconds) * bucketSeconds;
  const axis: number[] = [];

  for (let timestamp = start; timestamp <= end; timestamp += bucketSeconds) {
    axis.push(timestamp);
  }

  return axis;
}

import type { AnalyticsRangeKey } from './types/api';

export const ANALYTICS_RANGES: AnalyticsRangeKey[] = ['15m', '1h', '6h', '24h', '7d', '30d'];

export const MODEL_COLORS = [
  '#155e75',
  '#3b82f6',
  '#0f766e',
  '#b45309',
  '#7c3aed',
  '#e11d48',
  '#2563eb',
  '#059669',
  '#4f46e5',
  '#0891b2',
];

export const THEME_STORAGE_KEY = 'moraine-monitor-theme';

export const POLL_INTERVAL_MS = 10_000;

export const DEFAULT_ANALYTICS_META = 'Loading model analytics…';

export const ROW_LIMIT_OPTIONS = [10, 25, 50, 100];

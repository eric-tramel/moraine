import type { AnalyticsRangeKey } from './api';

export type ThemeMode = 'light' | 'dark';

export type StatTone = 'ok' | 'warn' | 'bad' | 'neutral';

export interface StatCard {
  label: string;
  value: string;
  tone?: StatTone;
}

export interface TableOption {
  value: string;
  label: string;
}

export interface RangeOption {
  key: AnalyticsRangeKey;
  label: string;
}

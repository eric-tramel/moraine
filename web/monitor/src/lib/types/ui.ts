import type { AnalyticsRangeKey } from './api';

export type ThemeMode = 'light' | 'dark';

export interface RangeOption {
  key: AnalyticsRangeKey;
  label: string;
}

import { writable } from 'svelte/store';
import type { AnalyticsRangeKey } from '../types/api';

export const selectedTableStore = writable<string | null>(null);
export const rowLimitStore = writable(25);
export const analyticsRangeStore = writable<AnalyticsRangeKey>('24h');

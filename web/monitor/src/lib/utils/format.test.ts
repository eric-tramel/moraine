import { describe, expect, it } from 'vitest';
import type { AnalyticsRange } from '../types/api';
import { buildBucketAxis } from './format';

const baseRange: AnalyticsRange = {
  key: '24h',
  label: 'Last 24h',
  window_seconds: 24 * 3600,
  bucket_seconds: 3600,
  from_unix: 1_700_000_000,
  to_unix: 1_700_007_200,
};

describe('buildBucketAxis', () => {
  it('keeps unix epoch zero as a valid boundary', () => {
    const axis = buildBucketAxis({
      ...baseRange,
      bucket_seconds: 900,
      from_unix: 0,
      to_unix: 1800,
    });

    expect(axis).toEqual([0, 900, 1800]);
  });

  it('returns an empty axis when timestamps are missing', () => {
    const axis = buildBucketAxis({
      ...baseRange,
      from_unix: null as unknown as number,
    });

    expect(axis).toEqual([]);
  });
});

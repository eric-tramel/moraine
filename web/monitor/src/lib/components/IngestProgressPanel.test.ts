import { render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import type { IngestAlert, IngestCondition, StatusResponse } from '../types/api';
import IngestProgressPanel from './IngestProgressPanel.svelte';

const observedAt = 1_700_000_103_000;
const unknownConditions: IngestCondition[] = [
  {
    condition_type: 'health',
    state: 'unknown',
    reason: 'heartbeat_clock_skew',
    observed_at_unix_ms: observedAt,
  },
  {
    condition_type: 'coverage',
    state: 'unknown',
    reason: 'progress_unavailable',
    observed_at_unix_ms: observedAt,
  },
  {
    condition_type: 'freshness',
    state: 'unknown',
    reason: 'progress_unavailable',
    observed_at_unix_ms: observedAt,
  },
  {
    condition_type: 'readiness',
    state: 'unknown',
    reason: 'conditions_unknown',
    observed_at_unix_ms: observedAt,
  },
];

function panelStatus(alerts: IngestAlert[]): StatusResponse {
  const latest = {
    ts_unix_ms: observedAt + 10_000,
    queue_depth: 0,
    files_active: 0,
    files_watched: 0,
    progress: null,
  };
  return {
    ok: true,
    ingestor: { present: true, alive: false, latest, age_seconds: 0 },
    ingest_status: {
      observed_at_unix_ms: observedAt,
      heartbeat: { table_present: true, latest },
      conditions: unknownConditions,
      alerts,
    },
  };
}

describe('IngestProgressPanel', () => {
  it('keeps every independent condition visible when progress cannot be decoded', () => {
    render(IngestProgressPanel, { props: { status: panelStatus([]) } });

    expect(screen.getByRole('heading', { name: 'Ingestion Progress' })).toBeVisible();
    for (const label of ['health', 'coverage', 'freshness', 'readiness']) {
      expect(screen.getByText(label)).toBeVisible();
    }
    expect(screen.getByText('Unavailable')).toBeVisible();
    expect(screen.getByText('No durable completion history requested or recorded yet.')).toBeVisible();
    expect(screen.getByText('No consecutive durable byte samples are available yet.')).toBeVisible();
  });

  it('removes recovered alerts on the next status response', async () => {
    const alert: IngestAlert = {
      code: 'progress_stalled',
      observed_at_unix_ms: observedAt,
    };
    const view = render(IngestProgressPanel, { props: { status: panelStatus([alert]) } });
    expect(screen.getByText('progress stalled')).toBeVisible();

    await view.rerender({ status: panelStatus([]) });

    expect(screen.queryByText('progress stalled')).not.toBeInTheDocument();
  });
});

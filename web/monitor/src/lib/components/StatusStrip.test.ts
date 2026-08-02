import { render, screen, within } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import type { IngestCondition, IngestProgressSnapshot, StatusResponse } from '../types/api';
import StatusStrip from './StatusStrip.svelte';

const observedAt = 1_700_000_103_000;

function condition(
  conditionType: IngestCondition['condition_type'],
  state: IngestCondition['state'],
  reason: string,
): IngestCondition {
  return {
    condition_type: conditionType,
    state,
    reason,
    observed_at_unix_ms: observedAt,
  };
}

function statusResponse(
  conditions: IngestCondition[],
  progress: IngestProgressSnapshot | null,
): StatusResponse {
  const latest = {
    ts_unix_ms: observedAt - 3_000,
    queue_depth: 0,
    files_active: 0,
    files_watched: 8,
    progress,
  };
  return {
    ok: true,
    ingestor: { present: true, alive: true, latest, age_seconds: 3 },
    ingest_status: {
      observed_at_unix_ms: observedAt,
      heartbeat: { table_present: true, latest },
      conditions,
      alerts: [],
    },
  };
}

describe('StatusStrip ingestion conditions', () => {
  it('renders all independent unknown conditions without decoded progress', () => {
    render(StatusStrip, {
      props: {
        status: statusResponse(
          [
            condition('health', 'unknown', 'heartbeat_clock_skew'),
            condition('coverage', 'unknown', 'progress_unavailable'),
            condition('freshness', 'unknown', 'progress_unavailable'),
            condition('readiness', 'unknown', 'conditions_unknown'),
          ],
          null,
        ),
      },
    });

    const ingestor = screen.getByText('Ingestor').parentElement!;
    for (const label of ['health', 'coverage', 'freshness', 'readiness']) {
      const chip = within(ingestor).getByText(label).closest('.ss-chip');
      expect(chip).toHaveClass('ss-warn');
      expect(chip).toHaveTextContent('unknown');
    }
  });

  it('shows file and byte completion separately with tone from the shared coverage condition', () => {
    const progress: IngestProgressSnapshot = {
      schema_version: 1,
      instance_id: 'run-a',
      run_started_unix_ms: observedAt - 60_000,
      snapshot_unix_ms: observedAt - 60_000,
      discovery_complete: true,
      queue_capacity: 16,
      sink_pending_rows: 0,
      sink_pending_bytes: 0,
      sink_retrying: false,
      oldest_pending_unix_ms: 0,
      last_durable_progress_unix_ms: observedAt,
      files_total: 8,
      files_completed: 8,
      bytes_total: 1_000,
      bytes_completed: 750,
      sources: [],
    };
    render(StatusStrip, {
      props: {
        status: statusResponse(
          [
            condition('health', 'true', 'heartbeat_recent'),
            condition('coverage', 'false', 'backfill_partial'),
            condition('freshness', 'true', 'progress_recent'),
            condition('readiness', 'false', 'retrieval_may_be_incomplete'),
          ],
          progress,
        ),
      },
    });

    const fileCoverage = screen.getByText('file coverage').closest('.ss-chip');
    const byteCoverage = screen.getByText('byte coverage').closest('.ss-chip');
    expect(fileCoverage).toHaveTextContent('8 / 8 · 100.0%');
    expect(fileCoverage).toHaveClass('ss-warn');
    expect(fileCoverage).not.toHaveClass('ss-ok');
    expect(byteCoverage).toHaveTextContent('750 / 1000 · 75.0%');
    expect(byteCoverage).toHaveClass('ss-warn');
    expect(screen.queryByText('backfill')).not.toBeInTheDocument();
  });
});

<script lang="ts">
  import type { HealthResponse, StatusResponse } from '../types/api';

  export let health: HealthResponse | null = null;
  export let healthError: string | null = null;
  export let status: StatusResponse | null = null;
  export let statusError: string | null = null;

  interface Chip {
    key: string;
    value: string;
    ok: boolean;
    tone?: 'warn' | 'bad';
  }

  function formatPing(ms: number | null | undefined): string {
    if (ms === null || ms === undefined) return 'n/a';
    return `${Number(ms).toFixed(2)}ms`;
  }

  function formatAge(seconds: number | null | undefined): string {
    if (seconds === null || seconds === undefined) return 'n/a';
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
    return `${Math.round(seconds / 3600)}h ago`;
  }

  function formatCoverage(completed: number, total: number): string {
    if (
      !Number.isFinite(completed) ||
      !Number.isFinite(total) ||
      total <= 0 ||
      completed < 0 ||
      completed > total
    ) {
      return 'n/a';
    }
    return `${((completed / total) * 100).toFixed(1)}%`;
  }

  function buildHealthChips(data: HealthResponse | null, error: string | null): Chip[] {
    if (error || !data?.ok) {
      return [
        {
          key: 'ClickHouse',
          value: error ?? data?.error ?? 'unavailable',
          ok: false,
          tone: 'bad',
        },
      ];
    }

    const url = data.url ?? 'unknown';
    const host = url.replace(/^https?:\/\//, '');
    const connections = data.connections?.total;
    const connectionsLabel =
      connections === null || connections === undefined ? 'n/a' : `${Number(connections).toLocaleString()}`;

    return [
      { key: 'ClickHouse', value: host, ok: true },
      { key: 'db', value: data.database ?? 'unknown', ok: false },
      { key: 'ver', value: data.version ?? 'n/a', ok: false },
      { key: 'ping', value: formatPing(data.ping_ms), ok: false },
      { key: 'conns', value: connectionsLabel, ok: false },
    ];
  }

  function buildIngestorChips(data: StatusResponse | null, error: string | null): Chip[] {
    if (error) {
      return [{ key: 'status', value: error, ok: false, tone: 'bad' }];
    }

    const ingestor = data?.ingestor;
    const ingestStatus = data?.ingest_status;
    if (!ingestor && !ingestStatus) {
      return [{ key: 'status', value: 'unknown', ok: false, tone: 'warn' }];
    }

    const chips: Chip[] = [];
    for (const conditionType of ['health', 'coverage', 'freshness', 'readiness'] as const) {
      const condition = ingestStatus?.conditions.find(
        (candidate) => candidate.condition_type === conditionType,
      );
      if (!condition) continue;

      const stateLabel =
        condition.state === 'unknown'
          ? 'unknown'
          : condition.state === 'true'
            ? {
                health: 'healthy',
                coverage: 'complete',
                freshness: 'fresh',
                readiness: 'ready',
              }[conditionType]
            : {
                health: 'unhealthy',
                coverage: 'partial',
                freshness: 'stale',
                readiness: 'not ready',
              }[conditionType];
      chips.push({
        key: conditionType,
        value: `${stateLabel} · ${condition.reason.replaceAll('_', ' ')}`,
        ok: condition.state === 'true',
        tone:
          condition.state === 'unknown'
            ? 'warn'
            : condition.state === 'false'
              ? conditionType === 'health'
                ? 'bad'
                : 'warn'
              : undefined,
      });
    }

    if (chips.length === 0 && ingestor) {
      if (!ingestor.present) {
        chips.push({ key: 'status', value: 'missing', ok: false, tone: 'warn' });
      } else if (!ingestor.latest) {
        chips.push({ key: 'status', value: 'waiting', ok: false, tone: 'warn' });
      } else if (ingestor.alive) {
        chips.push({ key: 'status', value: 'healthy', ok: true });
      } else {
        chips.push({ key: 'status', value: 'stale', ok: false, tone: 'warn' });
      }
    }

    if (ingestor?.age_seconds !== null && ingestor?.age_seconds !== undefined) {
      chips.push({ key: 'heartbeat', value: formatAge(ingestor.age_seconds), ok: false });
    }

    if (ingestor?.latest) {
      const queue = ingestor.latest.queue_depth;
      const capacity = ingestStatus?.heartbeat.latest?.progress?.queue_capacity;
      chips.push({
        key: 'queue pressure',
        value:
          queue === null || queue === undefined
            ? 'n/a'
            : `${queue} / ${capacity === null || capacity === undefined ? '?' : capacity}`,
        ok: false,
      });
      const active = ingestor.latest.files_active ?? '?';
      const watched = ingestor.latest.files_watched ?? '?';
      chips.push({ key: 'files active', value: `${active} / ${watched}`, ok: false });
    }

    const progress = ingestStatus?.heartbeat.latest?.progress;
    const coverageCondition = ingestStatus?.conditions.find(
      (condition) => condition.condition_type === 'coverage',
    );
    if (progress) {
      const coverageTone: Chip['tone'] =
        coverageCondition?.state === 'false'
          ? 'warn'
          : coverageCondition?.state === 'unknown'
            ? 'warn'
            : undefined;
      const coverageOk = coverageCondition?.state === 'true';
      const filePercent = formatCoverage(progress.files_completed, progress.files_total);
      const bytePercent = formatCoverage(progress.bytes_completed, progress.bytes_total);
      chips.push({
        key: 'file coverage',
        value: `${progress.files_completed} / ${progress.files_total} · ${filePercent}`,
        ok: coverageOk && progress.files_total > 0,
        tone: coverageTone,
      });
      chips.push({
        key: 'byte coverage',
        value: `${progress.bytes_completed} / ${progress.bytes_total} · ${bytePercent}`,
        ok: coverageOk && progress.bytes_total > 0,
        tone: coverageTone,
      });
    }

    if (ingestStatus?.eta) {
      chips.push({
        key: 'ETA',
        value: `${formatAge(ingestStatus.eta.low_seconds).replace(' ago', '')}–${formatAge(ingestStatus.eta.high_seconds).replace(' ago', '')}`,
        ok: false,
      });
    }
    if ((ingestStatus?.alerts.length ?? 0) > 0) {
      chips.push({
        key: 'alerts',
        value: String(ingestStatus?.alerts.length),
        ok: false,
        tone: 'bad',
      });
    }

    return chips;
  }

  $: healthChips = buildHealthChips(health, healthError);
  $: ingestorChips = buildIngestorChips(status, statusError);
</script>

<section class="panel status-strip" id="statusStrip">
  <div class="ss-group" id="healthGroup">
    <div class="ss-group-label">Health</div>
    <div class="ss-chips">
      {#each healthChips as chip (chip.key)}
        <span class="ss-chip" class:ss-ok={chip.ok} class:ss-warn={chip.tone === 'warn'} class:ss-bad={chip.tone === 'bad'}>
          {#if chip.ok}<span class="ss-dot"></span>{/if}
          <span class="ss-k">{chip.key}</span>
          <span class="ss-v mono">{chip.value}</span>
        </span>
      {/each}
    </div>
  </div>
  <div class="ss-divider" aria-hidden="true"></div>
  <div class="ss-group" id="ingestorGroup">
    <div class="ss-group-label">Ingestor</div>
    <div class="ss-chips">
      {#each ingestorChips as chip (chip.key)}
        <span class="ss-chip" class:ss-ok={chip.ok} class:ss-warn={chip.tone === 'warn'} class:ss-bad={chip.tone === 'bad'}>
          {#if chip.ok}<span class="ss-dot"></span>{/if}
          <span class="ss-k">{chip.key}</span>
          <span class="ss-v mono">{chip.value}</span>
        </span>
      {/each}
    </div>
  </div>
</section>

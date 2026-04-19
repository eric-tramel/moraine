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
    if (!ingestor) {
      return [{ key: 'status', value: 'unknown', ok: false, tone: 'warn' }];
    }

    let statusLabel = 'unknown';
    let statusOk = false;
    let tone: Chip['tone'];

    if (!ingestor.present) {
      statusLabel = 'missing';
      tone = 'warn';
    } else if (!ingestor.latest) {
      statusLabel = 'waiting';
      tone = 'warn';
    } else if (ingestor.alive) {
      statusLabel = 'healthy';
      statusOk = true;
    } else {
      statusLabel = 'stale';
      tone = 'warn';
    }

    const chips: Chip[] = [{ key: 'status', value: statusLabel, ok: statusOk, tone }];

    if (ingestor.age_seconds !== null && ingestor.age_seconds !== undefined) {
      chips.push({ key: 'heartbeat', value: formatAge(ingestor.age_seconds), ok: false });
    }

    if (ingestor.latest) {
      const queue = ingestor.latest.queue_depth;
      chips.push({
        key: 'queue',
        value: queue === null || queue === undefined ? 'n/a' : String(queue),
        ok: false,
      });
      const active = ingestor.latest.files_active ?? '?';
      const watched = ingestor.latest.files_watched ?? '?';
      chips.push({ key: 'files', value: `${active} / ${watched}`, ok: false });
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

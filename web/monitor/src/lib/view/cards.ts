import type { HealthResponse, StatusResponse } from '../types/api';
import type { StatCard } from '../types/ui';

export function buildHealthCards(data: HealthResponse): StatCard[] {
  if (!data.ok) {
    const failedConnections = data.connections?.error ? 'unavailable' : 'unknown';
    return [
      { label: 'ClickHouse', value: 'Unavailable', tone: 'bad' },
      { label: 'Reason', value: data.error || 'not available', tone: 'bad' },
      { label: 'Connections', value: failedConnections, tone: 'warn' },
    ];
  }

  const pingValue =
    data.ping_ms === null || data.ping_ms === undefined ? 'n/a' : `${Number(data.ping_ms).toFixed(2)} ms`;
  const connectionTotal = data.connections?.total;
  const connectionLabel =
    connectionTotal === null || connectionTotal === undefined
      ? 'unknown'
      : `${Number(connectionTotal).toLocaleString()} total`;

  return [
    { label: 'ClickHouse', value: data.url || 'unknown', tone: 'ok' },
    { label: 'Database', value: data.database || 'unknown', tone: 'ok' },
    { label: 'Version', value: data.version || 'n/a', tone: 'ok' },
    { label: 'Ping', value: pingValue, tone: 'ok' },
    { label: 'Connections', value: connectionLabel, tone: 'neutral' },
  ];
}

export function buildIngestorCards(data: StatusResponse): StatCard[] {
  if (!data.ingestor) {
    return [{ label: 'Ingestor', value: 'Unknown', tone: 'warn' }];
  }

  let status = 'Unknown';
  let tone: StatCard['tone'] = 'warn';

  if (!data.ingestor.present) {
    status = 'Table missing';
  } else if (!data.ingestor.latest) {
    status = 'No samples';
  } else if (data.ingestor.alive) {
    status = 'Healthy';
    tone = 'ok';
  } else {
    status = 'Stale';
  }

  const cards: StatCard[] = [{ label: 'Ingestor', value: status, tone }];

  if (data.ingestor.latest) {
    const heartbeatAge =
      data.ingestor.age_seconds === null || data.ingestor.age_seconds === undefined
        ? 'unknown'
        : `${data.ingestor.age_seconds}s`;

    const queueDepth = data.ingestor.latest.queue_depth;
    const queueDepthLabel = queueDepth === null || queueDepth === undefined ? 'unknown' : `${queueDepth}`;

    cards.push({ label: 'Heartbeat age', value: heartbeatAge, tone });
    cards.push({ label: 'Queue depth', value: queueDepthLabel, tone: 'neutral' });
    cards.push({
      label: 'Files',
      value: `${data.ingestor.latest.files_active ?? '?'} active / ${data.ingestor.latest.files_watched ?? '?'} watched`,
      tone: 'neutral',
    });

    return cards;
  }

  cards.push({
    label: 'Heartbeat',
    value: data.ingestor.present ? 'waiting for first row' : 'table not found',
    tone: 'neutral',
  });

  return cards;
}

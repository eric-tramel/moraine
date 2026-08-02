export type AnalyticsRangeKey = '15m' | '1h' | '6h' | '24h' | '7d' | '30d';

export interface ApiError {
  error?: string;
}

export interface ConnectionStats {
  total?: number | null;
  error?: string | null;
}

export interface HealthResponse {
  ok: boolean;
  url?: string;
  database?: string;
  version?: string | null;
  ping_ms?: number | null;
  connections?: ConnectionStats;
  ingestor?: IngestorStatus;
  error?: string;
}

export interface IngestorLatest {
  queue_depth?: number | null;
  files_active?: number | null;
  files_watched?: number | null;
  ts_unix_ms?: number;
  progress?: IngestProgressSnapshot | null;
  [key: string]: unknown;
}

export interface IngestorStatus {
  present: boolean;
  alive: boolean;
  latest: IngestorLatest | null;
  age_seconds: number | null;
}
export interface IngestProgressSnapshot {
  schema_version: number;
  instance_id: string;
  run_started_unix_ms: number;
  snapshot_unix_ms: number;
  discovery_complete: boolean;
  queue_capacity: number;
  sink_pending_rows: number;
  sink_pending_bytes: number;
  sink_retrying: boolean;
  oldest_pending_unix_ms: number;
  last_durable_progress_unix_ms: number;
  files_total: number;
  files_completed: number;
  bytes_total: number;
  bytes_completed: number;
  sources: IngestSourceProgress[];
}

export interface IngestSourceProgress {
  source_name: string;
  format: string;
  coverage_basis: 'bytes' | 'files' | 'unknown';
  files_total: number;
  files_completed: number;
  bytes_total: number;
  bytes_completed: number;
  coverage_degraded: boolean;
}

export interface IngestCondition {
  condition_type: 'health' | 'coverage' | 'freshness' | 'readiness';
  state: 'true' | 'false' | 'unknown';
  reason: string;
  observed_at_unix_ms: number;
}

export interface IngestAlert {
  code:
    | 'heartbeat_stale'
    | 'progress_stalled'
    | 'queue_saturated'
    | 'sink_retrying'
    | 'coverage_degraded';
  observed_at_unix_ms: number;
}

export interface IngestHistoryPoint {
  ts_unix_ms: number;
  queue_depth: number;
  files_active: number;
  queue_capacity: number;
  sink_pending_rows: number;
  sink_retrying: boolean;
  discovery_complete: boolean;
  files_total: number;
  files_completed: number;
  bytes_total: number;
  bytes_completed: number;
}

export interface IngestStatus {
  observed_at_unix_ms: number;
  heartbeat: {
    table_present: boolean;
    latest: IngestorLatest | null;
  };
  conditions: IngestCondition[];
  alerts: IngestAlert[];
  rate?: { bytes_per_second: number; sample_seconds: number } | null;
  eta?: { scope: string; low_seconds: number; high_seconds: number } | null;
  history?: IngestHistoryPoint[];
}


export interface StatusResponse {
  ok: boolean;
  ingestor?: IngestorStatus;
  ingest_status?: IngestStatus | null;
  error?: string;
}

export interface AnalyticsRange {
  key: AnalyticsRangeKey;
  label: string;
  window_seconds: number;
  bucket_seconds: number;
  from_unix: number;
  to_unix: number;
}

export interface TokenPoint {
  bucket_unix: number;
  model: string;
  endpoint_kind?: string;
  bucket?: string;
  tokens: number;
}

export interface TurnPoint {
  bucket_unix: number;
  model: string;
  turns: number;
}

export interface ConcurrentSessionsPoint {
  bucket_unix: number;
  concurrent_sessions: number;
}

export interface AnalyticsSeries {
  tokens: TokenPoint[];
  turns: TurnPoint[];
  concurrent_sessions: ConcurrentSessionsPoint[];
}

export interface AnalyticsResponse {
  ok: boolean;
  range: AnalyticsRange;
  series: AnalyticsSeries;
  error?: string;
}

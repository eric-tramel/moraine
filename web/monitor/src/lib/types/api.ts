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
  error?: string;
}

export interface IngestorLatest {
  queue_depth?: number | null;
  files_active?: number | null;
  files_watched?: number | null;
  [key: string]: unknown;
}

export interface IngestorStatus {
  present: boolean;
  alive: boolean;
  latest: IngestorLatest | null;
  age_seconds: number | null;
}

export interface StatusResponse {
  ok: boolean;
  ingestor?: IngestorStatus;
  error?: string;
}

export interface TableSummary {
  name: string;
  engine: string;
  is_temporary: number;
  rows: number;
}

export interface TablesResponse {
  ok: boolean;
  tables: TableSummary[];
  error?: string;
}

export interface TableColumn {
  name: string;
  type: string;
  default_expression: string;
}

export interface TableDetailResponse {
  ok: boolean;
  table: string;
  limit: number;
  schema: TableColumn[];
  rows: Array<Record<string, unknown>>;
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

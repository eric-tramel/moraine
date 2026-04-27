import type { ChartDataset } from 'chart.js';
import { MODEL_COLORS } from '../constants';
import type {
  AnalyticsRange,
  AnalyticsResponse,
  ConcurrentSessionsPoint,
  TokenPoint,
  TurnPoint,
} from '../types/api';
import { buildBucketAxis, formatBucketLabel, formatBucketSize } from '../utils/format';

export interface AnalyticsChartView {
  labels: string[];
  tokenDatasets: ChartDataset<'bar', number[]>[];
  turnDatasets: ChartDataset<'bar', number[]>[];
  concurrentDatasets: ChartDataset<'line', number[]>[];
  metaText: string;
  maxTicks: number;
}

function tokenSeriesLabel(point: TokenPoint): string {
  const model = point.model || 'unknown';
  const endpoint = point.endpoint_kind || 'generation';
  const bucket = point.bucket || 'tokens';
  return `${model} · ${endpoint} · ${bucket}`;
}

function mapSeriesPoints<T extends TokenPoint | TurnPoint>(
  points: T[],
  valueKey: 'tokens' | 'turns',
  labelForPoint: (point: T) => string,
): Map<string, Map<number, number>> {
  const seriesMap = new Map<string, Map<number, number>>();

  for (const point of points) {
    const label = labelForPoint(point);
    const bucket = Number(point.bucket_unix);
    const value =
      valueKey === 'tokens'
        ? Number((point as TokenPoint).tokens || 0)
        : Number((point as TurnPoint).turns || 0);

    if (!seriesMap.has(label)) {
      seriesMap.set(label, new Map<number, number>());
    }

    seriesMap.get(label)!.set(bucket, value);
  }

  return seriesMap;
}

function collectTopTokenSeries(tokens: TokenPoint[]): string[] {
  const totals = new Map<string, number>();

  for (const point of tokens) {
    const label = tokenSeriesLabel(point);
    totals.set(label, (totals.get(label) || 0) + Number(point.tokens || 0));
  }

  return [...totals.entries()]
    .sort((left, right) => right[1] - left[1])
    .map(([label]) => label)
    .slice(0, 8);
}

function collectTopModels(tokens: TokenPoint[], turns: TurnPoint[]): string[] {
  const models = new Map<string, number>();

  for (const point of tokens) {
    const model = point.model || 'unknown';
    models.set(model, (models.get(model) || 0) + Number(point.tokens || 0));
  }

  for (const point of turns) {
    const model = point.model || 'unknown';
    if (!models.has(model)) {
      models.set(model, 0);
    }
  }

  return [...models.entries()]
    .sort((left, right) => right[1] - left[1])
    .map(([model]) => model)
    .slice(0, 8);
}

function mapConcurrent(points: ConcurrentSessionsPoint[]): Map<number, number> {
  const values = new Map<number, number>();
  for (const point of points) {
    values.set(Number(point.bucket_unix), Number(point.concurrent_sessions || 0));
  }
  return values;
}

function maxTicksForRange(range: AnalyticsRange): number {
  return range.key === '7d' || range.key === '30d' ? 12 : 10;
}

export function buildAnalyticsView(data: AnalyticsResponse): AnalyticsChartView {
  const range = data.range;
  const bucketAxis = buildBucketAxis(range);

  const tokenPoints = data.series?.tokens || [];
  const turnPoints = data.series?.turns || [];
  const concurrentPoints = data.series?.concurrent_sessions || [];

  const tokenMap = mapSeriesPoints<TokenPoint>(tokenPoints, 'tokens', tokenSeriesLabel);
  const turnMap = mapSeriesPoints<TurnPoint>(turnPoints, 'turns', (point) => point.model || 'unknown');
  const concurrentMap = mapConcurrent(concurrentPoints);
  const tokenSeries = collectTopTokenSeries(tokenPoints);
  const models = collectTopModels(tokenPoints, turnPoints);

  const labels = bucketAxis.map((bucket) => formatBucketLabel(bucket, range.key));

  const tokenDatasets = tokenSeries.map((series, index) => {
    const color = MODEL_COLORS[index % MODEL_COLORS.length];
    return {
      label: series,
      data: bucketAxis.map((bucket) => tokenMap.get(series)?.get(bucket) ?? 0),
      borderColor: color,
      backgroundColor: `${color}26`,
      borderWidth: 1,
      borderRadius: 3,
    } satisfies ChartDataset<'bar', number[]>;
  });

  const turnDatasets = models.map((model, index) => {
    const color = MODEL_COLORS[index % MODEL_COLORS.length];
    return {
      label: model,
      data: bucketAxis.map((bucket) => turnMap.get(model)?.get(bucket) ?? 0),
      borderColor: color,
      backgroundColor: `${color}B0`,
      borderWidth: 1,
      borderRadius: 4,
    } satisfies ChartDataset<'bar', number[]>;
  });

  const concurrentDatasets: ChartDataset<'line', number[]>[] = [
    {
      label: 'Concurrent sessions',
      data: bucketAxis.map((bucket) => concurrentMap.get(bucket) ?? 0),
      borderColor: '#f59e0b',
      backgroundColor: '#f59e0b33',
      borderWidth: 2,
      tension: 0.2,
      pointRadius: 2,
      fill: true,
    },
  ];

  const modelCount = models.length;
  const metaText = `${range.label} • ${formatBucketSize(range.bucket_seconds)} buckets • ${modelCount} model${modelCount === 1 ? '' : 's'} • updated ${new Date().toLocaleTimeString()}`;

  return {
    labels,
    tokenDatasets,
    turnDatasets,
    concurrentDatasets,
    metaText,
    maxTicks: maxTicksForRange(range),
  };
}

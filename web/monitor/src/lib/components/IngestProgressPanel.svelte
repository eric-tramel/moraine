<script lang="ts">
  import type { Chart, ChartType } from 'chart.js';
  import { buildIngestChartView, type IngestChartView } from '../charts/ingest';
  import { createOrUpdateChart, destroyChart } from '../charts/chart';
  import type { IngestCondition, StatusResponse } from '../types/api';
  import type { ThemeMode } from '../types/ui';

  export let status: StatusResponse | null = null;
  export let theme: ThemeMode = 'light';

  let chartView: IngestChartView | null = null;

  $: ingestStatus = status?.ingest_status ?? null;
  $: latestProgress = ingestStatus?.heartbeat.latest?.progress ?? null;
  $: coverageCondition =
    ingestStatus?.conditions.find((condition) => condition.condition_type === 'coverage') ?? null;
  $: {
    theme;
    chartView = ingestStatus ? buildIngestChartView(ingestStatus) : null;
  }

  function stateLabel(condition: IngestCondition): string {
    if (condition.state === 'unknown') return 'unknown';
    if (condition.state === 'false') {
      if (condition.condition_type === 'health') return 'unhealthy';
      if (condition.condition_type === 'coverage') return 'partial';
      if (condition.condition_type === 'freshness') return 'stale';
      return 'not ready';
    }
    if (condition.condition_type === 'health') return 'healthy';
    if (condition.condition_type === 'coverage') return 'complete';
    if (condition.condition_type === 'freshness') return 'fresh';
    return 'ready';
  }

  function progressPercent(completed: number, total: number): string | null {
    if (
      !Number.isFinite(completed) ||
      !Number.isFinite(total) ||
      total <= 0 ||
      completed < 0 ||
      completed > total
    ) {
      return null;
    }
    return `${((completed / total) * 100).toFixed(1)}%`;
  }

  type IngestChartKind = 'completion' | 'throughput';

  interface IngestChartBinding {
    kind: IngestChartKind;
    theme: ThemeMode;
    view: IngestChartView | null;
  }

  function renderChart(canvas: HTMLCanvasElement, initial: IngestChartBinding) {
    let chart: Chart<ChartType> | null = null;

    function update(binding: IngestChartBinding): void {
      void binding.theme;
      if (!binding.view) {
        destroyChart(chart);
        chart = null;
        return;
      }
      const completion = binding.kind === 'completion';
      chart = createOrUpdateChart(
        chart,
        canvas,
        'line',
        binding.view.labels,
        completion ? binding.view.completionDatasets : binding.view.throughputDatasets,
        completion ? 'Coverage' : 'Bytes/s',
        completion
          ? { maxTicks: 8, yTickFormatter: (value) => `${value}%` }
          : { maxTicks: 8 },
      );
    }

    update(initial);
    return {
      update,
      destroy(): void {
        destroyChart(chart);
      },
    };
  }
</script>

{#if ingestStatus}
  <section class="panel ingest-progress" aria-labelledby="ingestProgressTitle">
    <div class="ingest-head">
      <div>
        <p class="eyebrow">Durable checkpoint telemetry</p>
        <h2 id="ingestProgressTitle">Ingestion Progress</h2>
      </div>
      <div class="eta-block">
        <span>ETA RANGE</span>
        {#if ingestStatus.eta}
          <strong>{ingestStatus.eta.low_seconds}–{ingestStatus.eta.high_seconds}s</strong>
          <small>{ingestStatus.eta.scope.replaceAll('_', ' ')} · stable file-byte rate</small>
        {:else}
          <strong>Unavailable</strong>
          <small>Requires stable durable file-byte throughput</small>
        {/if}
      </div>
    </div>

    <div class="condition-rail" aria-label="Independent ingestion conditions">
      {#each ingestStatus.conditions as condition (condition.condition_type)}
        <article
          class:condition-bad={condition.state === 'false' && condition.condition_type === 'health'}
          class:condition-warn={condition.state === 'false' && condition.condition_type !== 'health'}
          class:condition-unknown={condition.state === 'unknown'}
        >
          <span>{condition.condition_type}</span>
          <strong>{stateLabel(condition)}</strong>
          <code>{condition.reason}</code>
        </article>
      {/each}
    </div>

    {#if ingestStatus.alerts.length > 0}
      <div class="alert-ribbon" role="status">
        <strong>{ingestStatus.alerts.length} active ingestion alert{ingestStatus.alerts.length === 1 ? '' : 's'}</strong>
        <span>{ingestStatus.alerts.map((alert) => alert.code.replaceAll('_', ' ')).join(' · ')}</span>
      </div>
    {/if}

    <div class="chart-grid ingest-chart-grid">
      <article class="chart-card">
        <h3>Durable Snapshot Completion</h3>
        {#if chartView}
          <div class="chart-wrap">
            <canvas
              use:renderChart={{ kind: 'completion', theme, view: chartView }}
              aria-label="File and byte completion history"
            ></canvas>
          </div>
        {:else}
          <p class="chart-empty">No durable completion history requested or recorded yet.</p>
        {/if}
      </article>
      <article class="chart-card">
        <h3>Durable Checkpoint Throughput</h3>
        {#if chartView}
          <div class="chart-wrap">
            <canvas
              use:renderChart={{ kind: 'throughput', theme, view: chartView }}
              aria-label="Durable checkpoint byte throughput history"
            ></canvas>
          </div>
        {:else}
          <p class="chart-empty">No consecutive durable byte samples are available yet.</p>
        {/if}
        {#if ingestStatus.rate}
          <p class="chart-note">
            Stable window: {Math.round(ingestStatus.rate.bytes_per_second).toLocaleString()} bytes/s
            over {ingestStatus.rate.sample_seconds}s
          </p>
        {/if}
      </article>
    </div>

    {#if latestProgress && latestProgress.sources.length > 0}
      <div class="source-grid" aria-label="Progress by ingest source">
        {#each latestProgress.sources as source (source.source_name)}
          <article
            class:coverage-complete={coverageCondition?.state === 'true'}
            class:coverage-partial={coverageCondition?.state === 'false'}
            class:coverage-unknown={coverageCondition?.state === 'unknown' || !coverageCondition}
          >
            <div class="source-head">
              <strong>{source.source_name}</strong>
              <span>{source.format}</span>
            </div>
            <div class="coverage-row">
              <div class="source-foot">
                <span>files · {source.files_completed}/{source.files_total}</span>
                <strong>{source.coverage_basis === 'unknown'
                  ? 'n/a'
                  : (progressPercent(source.files_completed, source.files_total) ?? 'n/a')}</strong>
              </div>
              <div class="meter" aria-hidden="true">
                <i style={`width: ${source.coverage_basis === 'unknown'
                  ? '0%'
                  : (progressPercent(source.files_completed, source.files_total) ?? '0%')}`}></i>
              </div>
            </div>
            <div class="coverage-row">
              <div class="source-foot">
                <span>bytes · {source.bytes_completed}/{source.bytes_total}</span>
                <strong>{source.coverage_basis === 'bytes'
                  ? (progressPercent(source.bytes_completed, source.bytes_total) ?? 'n/a')
                  : 'n/a'}</strong>
              </div>
              <div class="meter" aria-hidden="true">
                <i style={`width: ${source.coverage_basis === 'bytes'
                  ? (progressPercent(source.bytes_completed, source.bytes_total) ?? '0%')
                  : '0%'}`}></i>
              </div>
            </div>
            {#if source.coverage_degraded}<small>Approximate mutation coverage</small>{/if}
          </article>
        {/each}
      </div>
    {/if}
  </section>
{/if}

<style>
  .ingest-progress { overflow: hidden; }
  .ingest-head { display: flex; align-items: end; justify-content: space-between; gap: 1rem; margin-bottom: 1rem; }
  .ingest-head h2 { margin: 0; }
  .eyebrow { margin: 0 0 0.2rem; color: var(--subtle); font: 600 0.67rem/1.2 'IBM Plex Mono', monospace; letter-spacing: 0.12em; text-transform: uppercase; }
  .eta-block { display: grid; justify-items: end; max-width: 22rem; text-align: right; }
  .eta-block span { color: var(--subtle); font: 600 0.65rem 'IBM Plex Mono', monospace; letter-spacing: 0.1em; }
  .eta-block strong { color: var(--primary); font: 700 1.15rem 'IBM Plex Mono', monospace; }
  .eta-block small { color: var(--subtle); font: 0.67rem 'IBM Plex Mono', monospace; }
  .condition-rail { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 0.55rem; margin-bottom: 0.75rem; }
  .condition-rail article { display: grid; gap: 0.12rem; padding: 0.65rem 0.75rem; border: 1px solid color-mix(in srgb, var(--good) 42%, var(--line)); border-left: 3px solid var(--good); border-radius: 0.55rem; background: var(--panel-alt); }
  .condition-rail article.condition-bad { border-left-color: var(--bad); }
  .condition-rail article.condition-warn,
  .condition-rail article.condition-unknown { border-left-color: var(--warn); }
  .condition-rail span { color: var(--subtle); font: 600 0.66rem 'IBM Plex Mono', monospace; text-transform: uppercase; }
  .condition-rail strong { font-size: 0.88rem; text-transform: capitalize; }
  .condition-rail code { color: var(--subtle); font-size: 0.68rem; overflow-wrap: anywhere; }
  .alert-ribbon { display: flex; justify-content: space-between; gap: 1rem; margin-bottom: 0.75rem; padding: 0.55rem 0.75rem; border: 1px solid color-mix(in srgb, var(--bad) 50%, var(--line)); border-radius: 0.5rem; background: color-mix(in srgb, var(--bad) 8%, var(--panel)); color: var(--text); font-size: 0.78rem; }
  .alert-ribbon span { color: var(--subtle); text-transform: capitalize; }
  .ingest-chart-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .chart-empty { display: grid; min-height: 12rem; margin: 0; place-items: center; color: var(--subtle); font: 0.75rem 'IBM Plex Mono', monospace; text-align: center; }
  .chart-note { margin: 0.4rem 0 0; color: var(--subtle); font: 0.67rem 'IBM Plex Mono', monospace; }
  .source-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(175px, 1fr)); gap: 0.55rem; margin-top: 0.75rem; }
  .source-grid article { --coverage-tone: var(--warn); padding: 0.65rem; border: 1px solid var(--line); border-left: 3px solid var(--coverage-tone); border-radius: 0.55rem; background: var(--surface); }
  .source-grid article.coverage-complete { --coverage-tone: var(--good); }
  .source-grid article.coverage-unknown { --coverage-tone: var(--line-strong); }
  .source-head, .source-foot { display: flex; align-items: baseline; justify-content: space-between; gap: 0.5rem; }
  .source-head span, .source-foot, .source-grid small { color: var(--subtle); font: 0.67rem 'IBM Plex Mono', monospace; }
  .coverage-row { margin-top: 0.65rem; }
  .meter { height: 4px; margin-top: 0.3rem; overflow: hidden; border-radius: 999px; background: var(--line); }
  .meter i { display: block; height: 100%; border-radius: inherit; background: var(--coverage-tone); }
  .source-grid small { display: block; margin-top: 0.35rem; color: var(--warn); }
  @media (max-width: 760px) {
    .ingest-head { align-items: start; flex-direction: column; }
    .eta-block { justify-items: start; text-align: left; }
    .condition-rail, .ingest-chart-grid { grid-template-columns: 1fr; }
    .alert-ribbon { flex-direction: column; gap: 0.2rem; }
  }
</style>

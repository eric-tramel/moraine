<script lang="ts">
  import { createEventDispatcher, onDestroy } from 'svelte';
  import type { Chart, ChartType } from 'chart.js';
  import { ANALYTICS_RANGES, DEFAULT_ANALYTICS_META } from '../constants';
  import { buildAnalyticsView } from '../charts/analytics';
  import { createOrUpdateChart, destroyChart } from '../charts/chart';
  import { formatCompactNumber } from '../utils/format';
  import type { AnalyticsRangeKey, AnalyticsResponse } from '../types/api';
  import type { ThemeMode } from '../types/ui';

  export let payload: AnalyticsResponse | null = null;
  export let selectedRange: AnalyticsRangeKey = '24h';
  export let errorMessage: string | null = null;
  export let theme: ThemeMode = 'light';

  const dispatch = createEventDispatcher<{
    rangeChange: AnalyticsRangeKey;
  }>();

  let tokensCanvas: HTMLCanvasElement;
  let turnsCanvas: HTMLCanvasElement;
  let concurrentCanvas: HTMLCanvasElement;

  let tokensChart: Chart<ChartType> | null = null;
  let turnsChart: Chart<ChartType> | null = null;
  let concurrentChart: Chart<ChartType> | null = null;

  let metaText = DEFAULT_ANALYTICS_META;
  let chartView = payload?.ok ? buildAnalyticsView(payload) : null;

  function renderCharts(): void {
    if (!chartView || !tokensCanvas || !turnsCanvas || !concurrentCanvas) {
      return;
    }

    tokensChart = createOrUpdateChart(
      tokensChart,
      tokensCanvas,
      'bar',
      chartView.labels,
      chartView.tokenDatasets,
      'Generation Tokens',
      {
        stacked: true,
        maxTicks: chartView.maxTicks,
        yTickFormatter: formatCompactNumber,
      },
    );

    turnsChart = createOrUpdateChart(
      turnsChart,
      turnsCanvas,
      'bar',
      chartView.labels,
      chartView.turnDatasets,
      'Turns',
      {
        stacked: false,
        maxTicks: chartView.maxTicks,
      },
    );

    concurrentChart = createOrUpdateChart(
      concurrentChart,
      concurrentCanvas,
      'line',
      chartView.labels,
      chartView.concurrentDatasets,
      'Sessions',
      {
        stacked: false,
        maxTicks: chartView.maxTicks,
      },
    );
  }

  $: {
    if (payload?.ok) {
      chartView = buildAnalyticsView(payload);
      metaText = chartView.metaText;
    } else {
      chartView = null;
      metaText = errorMessage || DEFAULT_ANALYTICS_META;
    }
  }

  $: if (chartView) {
    theme;
    renderCharts();
  }

  onDestroy(() => {
    destroyChart(tokensChart);
    destroyChart(turnsChart);
    destroyChart(concurrentChart);
  });
</script>

<section class="panel">
  <div class="analytics-head">
    <h2>Live Analytics</h2>
    <div id="analyticsRanges" class="range-group" role="group" aria-label="Analytics range">
      {#each ANALYTICS_RANGES as range}
        <button
          type="button"
          class:active={selectedRange === range}
          on:click={() => dispatch('rangeChange', range)}
        >
          {range}
        </button>
      {/each}
    </div>
  </div>
  <p id="analyticsMeta" class="muted">{metaText}</p>

  <div class="chart-grid">
    <article class="chart-card">
      <h3>Generation Tokens Per Model</h3>
      <div class="chart-wrap">
        <canvas id="tokensChart" bind:this={tokensCanvas}></canvas>
      </div>
    </article>

    <article class="chart-card">
      <h3>Turns Per Model</h3>
      <div class="chart-wrap">
        <canvas id="turnsChart" bind:this={turnsCanvas}></canvas>
      </div>
    </article>

    <article class="chart-card">
      <h3>Concurrent Sessions</h3>
      <div class="chart-wrap">
        <canvas id="concurrentSessionsChart" bind:this={concurrentCanvas}></canvas>
      </div>
    </article>
  </div>
</section>

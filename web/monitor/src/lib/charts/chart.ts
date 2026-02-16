import Chart from 'chart.js/auto';
import type { Chart as ChartInstance, ChartDataset, ChartType } from 'chart.js';
import { chartTheme } from './theme';

export interface ChartUpdateOptions {
  stacked?: boolean;
  maxTicks?: number;
  yTickFormatter?: ((value: number) => string) | null;
}

export function createOrUpdateChart(
  currentChart: ChartInstance<ChartType> | null,
  canvas: HTMLCanvasElement,
  type: ChartType,
  labels: string[],
  datasets: ChartDataset<ChartType, number[]>[],
  yTitle: string,
  options: ChartUpdateOptions = {},
): ChartInstance<ChartType> {
  const palette = chartTheme();
  const stacked = options.stacked === true;
  const maxTicks = Number(options.maxTicks || 10);
  const yTickFormatter = options.yTickFormatter;

  if (!currentChart) {
    return new Chart(canvas.getContext('2d')!, {
      type,
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: 'nearest', intersect: false },
        scales: {
          x: {
            stacked,
            ticks: { maxTicksLimit: maxTicks, color: palette.text },
            grid: { color: palette.grid },
          },
          y: {
            stacked,
            beginAtZero: true,
            ticks: {
              color: palette.text,
              callback: yTickFormatter
                ? (value: string | number) => yTickFormatter(Number(value))
                : undefined,
            },
            grid: { color: palette.grid },
            title: { display: true, text: yTitle, color: palette.text },
          },
        },
        plugins: {
          legend: {
            position: 'bottom',
            labels: {
              boxWidth: 10,
              boxHeight: 10,
              usePointStyle: true,
              color: palette.text,
            },
          },
          tooltip: { enabled: true },
        },
      },
    }) as ChartInstance<ChartType>;
  }

  (currentChart.config as { type: ChartType }).type = type;
  currentChart.data.labels = labels;
  currentChart.data.datasets = datasets as ChartDataset<ChartType, number[]>[];

  const scaleOptions = currentChart.options?.scales as
    | Record<string, { stacked?: boolean; ticks?: Record<string, unknown>; grid?: Record<string, unknown>; title?: Record<string, unknown> }>
    | undefined;

  if (scaleOptions?.x) {
    scaleOptions.x.stacked = stacked;
    if (scaleOptions.x.ticks) {
      scaleOptions.x.ticks.maxTicksLimit = maxTicks;
      scaleOptions.x.ticks.color = palette.text;
    }
    if (scaleOptions.x.grid) {
      scaleOptions.x.grid.color = palette.grid;
    }
  }

  if (scaleOptions?.y) {
    scaleOptions.y.stacked = stacked;
    if (scaleOptions.y.ticks) {
      scaleOptions.y.ticks.color = palette.text;
      scaleOptions.y.ticks.callback = yTickFormatter
        ? (value: string | number) => yTickFormatter(Number(value))
        : undefined;
    }
    if (scaleOptions.y.grid) {
      scaleOptions.y.grid.color = palette.grid;
    }
    if (scaleOptions.y.title) {
      scaleOptions.y.title.color = palette.text;
    }
  }

  if (currentChart.options?.plugins?.legend?.labels) {
    currentChart.options.plugins.legend.labels.color = palette.text;
  }

  currentChart.update();
  return currentChart;
}

export function destroyChart(chart: ChartInstance<ChartType> | null): void {
  chart?.destroy();
}

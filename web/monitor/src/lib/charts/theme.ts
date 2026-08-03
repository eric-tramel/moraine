export interface ChartPalette {
  text: string;
  grid: string;
  good: string;
  primary: string;
  warn: string;
}

function readCssVar(name: string, fallback: string): string {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

export function chartTheme(): ChartPalette {
  return {
    text: readCssVar('--chart-text', '#435568'),
    grid: readCssVar('--chart-grid', '#e8eff4'),
    good: readCssVar('--good', '#0f766e'),
    primary: readCssVar('--primary', '#155e75'),
    warn: readCssVar('--warn', '#b45309'),
  };
}

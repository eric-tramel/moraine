export interface ChartPalette {
  text: string;
  grid: string;
}

function readCssVar(name: string, fallback: string): string {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

export function chartTheme(): ChartPalette {
  return {
    text: readCssVar('--chart-text', '#435568'),
    grid: readCssVar('--chart-grid', '#e8eff4'),
  };
}

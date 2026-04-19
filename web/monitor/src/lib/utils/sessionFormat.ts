export function fmtDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const s = Math.round(ms / 100) / 10;
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rs = Math.round(s - m * 60);
  if (m < 60) return rs ? `${m}m ${rs}s` : `${m}m`;
  const h = Math.floor(m / 60);
  const rm = m - h * 60;
  return rm ? `${h}h ${rm}m` : `${h}h`;
}

export function fmtTokens(n: number): string {
  if (n >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/, '') + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/, '') + 'k';
  return String(n);
}

export function fmtRelative(ts: number, now: number = Date.now()): string {
  const d = Math.max(0, now - ts);
  if (d < 60_000) return 'just now';
  if (d < 3_600_000) return Math.floor(d / 60_000) + 'm ago';
  if (d < 86_400_000) return Math.floor(d / 3_600_000) + 'h ago';
  return Math.floor(d / 86_400_000) + 'd ago';
}

export function fmtClock(ts: number): string {
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

export function fmtDate(ts: number): string {
  return new Date(ts).toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function fmtDayLabel(ts: number): string {
  return new Date(ts).toLocaleDateString([], { weekday: 'short', month: 'short', day: 'numeric' });
}

export function fmtShortDate(ts: number): string {
  return new Date(ts).toLocaleDateString([], { year: '2-digit', month: '2-digit', day: '2-digit' });
}

export function summarizeArgs(args: Record<string, unknown> | undefined | null): string {
  if (!args || typeof args !== 'object') return '';
  const entries = Object.entries(args).slice(0, 2);
  return entries
    .map(([k, v]) => {
      let s: string;
      if (typeof v === 'string') {
        s = v.length > 42 ? v.slice(0, 40) + '\u2026' : v;
      } else {
        s = JSON.stringify(v);
      }
      return `${k}: ${s}`;
    })
    .join(', ');
}

export const ACTIVE_WINDOW_MS = 60_000;

export function isSessionActive(endedAt: number, now: number = Date.now()): boolean {
  return now - endedAt < ACTIVE_WINDOW_MS;
}

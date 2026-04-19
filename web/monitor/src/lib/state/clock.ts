import { readable } from 'svelte/store';

const TICK_INTERVAL_MS = 5_000;

export const nowStore = readable(Date.now(), (set) => {
  const interval = setInterval(() => set(Date.now()), TICK_INTERVAL_MS);
  return () => clearInterval(interval);
});

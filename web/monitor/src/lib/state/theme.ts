import { get, writable } from 'svelte/store';
import { THEME_STORAGE_KEY } from '../constants';
import type { ThemeMode } from '../types/ui';

export const themeStore = writable<ThemeMode>('light');

function readStoredTheme(): ThemeMode | null {
  try {
    const value = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (value === 'light' || value === 'dark') {
      return value;
    }
  } catch {
    // ignore localStorage access errors
  }

  return null;
}

function getPreferredTheme(): ThemeMode {
  const stored = readStoredTheme();
  if (stored) {
    return stored;
  }

  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function applyTheme(theme: ThemeMode): void {
  document.documentElement.setAttribute('data-theme', theme);
}

function persistTheme(theme: ThemeMode): void {
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch {
    // ignore localStorage access errors
  }
}

export function setTheme(theme: ThemeMode): void {
  applyTheme(theme);
  persistTheme(theme);
  themeStore.set(theme);
}

export function initializeTheme(): void {
  setTheme(getPreferredTheme());
}

export function toggleTheme(): void {
  const nextTheme = get(themeStore) === 'dark' ? 'light' : 'dark';
  setTheme(nextTheme);
}

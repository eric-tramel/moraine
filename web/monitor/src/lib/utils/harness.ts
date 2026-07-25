import type { Harness } from '../types/sessions';

/**
 * Display identity for a harness id.
 *
 * The session feed carries `harness` as a plain id (or null when no event
 * recorded one). Colour and initials are presentation, so they are derived here
 * rather than served: a stable hash keeps an unknown harness visually distinct
 * and consistent between the card, the badge and the filter menu.
 */

const UNKNOWN_HUE = 220;

export const UNKNOWN_HARNESS_LABEL = 'unknown';

function hueFor(id: string): number {
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    hash = (hash * 31 + id.charCodeAt(i)) | 0;
  }
  return Math.abs(hash) % 360;
}

function initials(label: string): string {
  const parts = label.split(/[^a-z0-9]+/i).filter(Boolean);
  if (parts.length === 0) return '??';
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[1][0]).toUpperCase();
}

export function harnessDescriptor(id: string | null | undefined): Harness {
  const trimmed = id?.trim();
  if (!trimmed) {
    return { id: '', label: UNKNOWN_HARNESS_LABEL, short: '??', hue: UNKNOWN_HUE };
  }
  return { id: trimmed, label: trimmed, short: initials(trimmed), hue: hueFor(trimmed) };
}

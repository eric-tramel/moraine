import { derived, writable } from 'svelte/store';
import type { Session, SessionsFilter } from '../types/sessions';

export const sessionsStore = writable<Session[]>([]);
export const sessionsLoadingStore = writable<boolean>(false);
export const sessionsErrorStore = writable<string | null>(null);

export const sessionsFilterStore = writable<SessionsFilter>({
  query: '',
  model: 'all',
  status: 'all',
  harness: 'all',
});

export const activeSessionIdStore = writable<string | null>(null);

export function filterSessions(sessions: Session[], filter: SessionsFilter): Session[] {
  const q = filter.query.trim().toLowerCase();
  return sessions.filter((s) => {
    if (filter.model !== 'all' && !s.models.includes(filter.model)) return false;
    if (filter.status !== 'all' && s.status !== filter.status) return false;
    if (filter.harness !== 'all' && s.harness.id !== filter.harness) return false;
    if (!q) return true;
    if (s.title.toLowerCase().includes(q)) return true;
    if (s.id.toLowerCase().includes(q)) return true;
    if (s.harness.label.toLowerCase().includes(q)) return true;
    if (s.tags.some((tag) => tag.toLowerCase().includes(q))) return true;
    for (const turn of s.turns) {
      for (const step of turn.steps) {
        if ('text' in step && step.text && step.text.toLowerCase().includes(q)) return true;
        if (step.kind === 'tool_call' && step.tool.toLowerCase().includes(q)) return true;
      }
    }
    return false;
  });
}

export const filteredSessionsStore = derived(
  [sessionsStore, sessionsFilterStore],
  ([$sessions, $filter]) => {
    const filtered = filterSessions($sessions, $filter);
    return [...filtered].sort((a, b) => b.endedAt - a.endedAt);
  },
);

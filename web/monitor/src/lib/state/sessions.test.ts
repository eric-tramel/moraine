import { describe, expect, it } from 'vitest';
import type { SessionSummary, SessionsFilter } from '../types/sessions';
import { filterSessions } from './sessions';

function summary(overrides: Partial<SessionSummary> = {}): SessionSummary {
  return {
    id: 'session-1',
    title: 'Inspect the repository',
    displayLabel: 'Inspect the repository',
    harness: 'codex',
    source: 'ci-codex',
    inferenceProvider: 'openai',
    mode: 'tool_calling',
    startedAt: 1_700_000_000_000,
    endedAt: 1_700_000_001_000,
    status: 'completed',
    turnCount: 3,
    eventCount: 12,
    toolCallCount: 2,
    sessionSlug: 'inspect-the-repository',
    sessionSummary: 'Walked the workspace manifest',
    ...overrides,
  };
}

function filter(overrides: Partial<SessionsFilter> = {}): SessionsFilter {
  return { query: '', status: 'all', harness: 'all', ...overrides };
}

describe('filterSessions', () => {
  it('matches the labels and identifiers the feed actually carries', () => {
    const sessions = [
      summary({ id: 'a' }),
      // No title: the server's synthesized label is the only thing to match on.
      summary({
        id: 'b',
        title: null,
        displayLabel: 'codex, tool calling, Feb 16 12:00 UTC, 3 turns',
        sessionSlug: null,
        sessionSummary: null,
      }),
      summary({
        id: 'sess-9f3',
        title: 'Unrelated',
        displayLabel: 'Unrelated',
        sessionSlug: 'unrelated',
        sessionSummary: 'ingest backfill',
      }),
    ];

    expect(filterSessions(sessions, filter({ query: 'repository' })).map((s) => s.id)).toEqual(['a']);
    expect(filterSessions(sessions, filter({ query: 'tool calling' })).map((s) => s.id)).toEqual(['b']);
    expect(filterSessions(sessions, filter({ query: 'backfill' })).map((s) => s.id)).toEqual([
      'sess-9f3',
    ]);
    // The id itself is matchable, which is how an operator pastes one in.
    expect(filterSessions(sessions, filter({ query: '9f3' })).map((s) => s.id)).toEqual(['sess-9f3']);
  });

  it('cannot match message content, because the feed carries none', () => {
    // The removed transcript branch searched `turn.steps[].text`. Nothing in a
    // summary holds message bodies, so a content query matches nothing — which
    // is why the input is labelled as filtering loaded sessions, not prompts.
    const sessions = [summary({ id: 'a' })];
    expect(filterSessions(sessions, filter({ query: 'workspace = true' }))).toEqual([]);
  });

  it('narrows by the two statuses the server can produce', () => {
    const sessions = [
      summary({ id: 'a', status: 'active' }),
      summary({ id: 'b', status: 'completed' }),
    ];
    expect(filterSessions(sessions, filter({ status: 'active' })).map((s) => s.id)).toEqual(['a']);
    expect(filterSessions(sessions, filter({ status: 'completed' })).map((s) => s.id)).toEqual(['b']);
  });

  it('treats a session with no recorded harness as matching no harness filter', () => {
    const sessions = [summary({ id: 'a', harness: null }), summary({ id: 'b', harness: 'codex' })];
    expect(filterSessions(sessions, filter({ harness: 'codex' })).map((s) => s.id)).toEqual(['b']);
  });
});

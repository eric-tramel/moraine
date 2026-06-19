import { render, screen, within } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import type { Turn } from '../../types/sessions';
import TurnViz from './TurnViz.svelte';

const piTurn: Turn = {
  idx: 1,
  model: 'claude-sonnet-4-5',
  startedAt: 1_700_000_000_000,
  endedAt: 1_700_000_003_000,
  durationMs: 3_000,
  promptTokens: 12,
  completionTokens: 24,
  totalTokens: 36,
  toolCalls: 1,
  steps: [
    {
      kind: 'user',
      at: 1_700_000_000_000,
      text: 'inspect the trace',
    },
    {
      kind: 'thinking',
      at: 1_700_000_000_600,
      text: 'I should inspect the file before editing.',
      durationMs: 500,
    },
    {
      kind: 'assistant',
      at: 1_700_000_001_200,
      text: 'I will inspect the file.',
      tokens: 8,
      durationMs: 400,
    },
    {
      kind: 'tool_call',
      at: 1_700_000_001_500,
      tool: 'read_file',
      args: { path: 'web/monitor/src/lib/components/sessions/TurnViz.svelte' },
      latencyMs: 275,
      result: 'contents',
      resultAt: 1_700_000_001_775,
      status: 'ok',
    },
  ],
};

describe('TurnViz', () => {
  it('labels thinking and assistant as distinct rows in trace view', () => {
    const { container } = render(TurnViz, {
      props: {
        turn: piTurn,
        variant: 'trace',
      },
    });

    const labels = Array.from(container.querySelectorAll('.mv-tr-label')).map((label) =>
      label.textContent?.trim(),
    );

    expect(labels).toEqual(['user', 'thinking', 'assistant', 'read_file']);
    expect(labels.filter((label) => label === 'assistant')).toHaveLength(1);
    expect(screen.getByText('thinking · 500ms')).toBeInTheDocument();
  });

  it('labels thinking and assistant as distinct nodes in timeline view', () => {
    const { container } = render(TurnViz, {
      props: {
        turn: piTurn,
        variant: 'timeline',
      },
    });

    const timeline = container.querySelector('.mv-turn-timeline');
    expect(timeline).not.toBeNull();

    const labels = Array.from(
      within(timeline as HTMLElement).getAllByText(/^(user|thinking|assistant|tool)$/),
    ).map((label) => label.textContent?.trim());

    expect(labels).toEqual(['user', 'thinking', 'assistant', 'tool']);
    expect(labels.filter((label) => label === 'assistant')).toHaveLength(1);
  });
});

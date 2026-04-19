import type {
  Harness,
  Session,
  SessionStatus,
  Step,
  ToolCallStep,
  Turn,
} from '../types/sessions';

const MODELS = [
  'claude-sonnet-4-5',
  'claude-haiku-4-5',
  'claude-opus-4-1',
  'gpt-5.1-mini',
];

const TOOLS = [
  'read_file',
  'write_file',
  'str_replace_edit',
  'grep',
  'list_files',
  'run_script',
  'web_fetch',
  'web_search',
  'save_screenshot',
  'show_html',
];

const TAGS = ['code-review', 'triage', 'refactor', 'debug', 'design', 'research', 'migration'];

const PROMPTS = [
  'Add pagination to the session list API and update the Svelte client to use cursor-based navigation.',
  'We\u2019re seeing ingestor heartbeat stale around 3am UTC \u2014 can you trace the last three occurrences?',
  'Refactor the analytics panel so the three charts share a single theme hook and re-render on theme change.',
  'Draft a migration plan to move from single-node ClickHouse to a replicated cluster without downtime.',
  'Investigate why turns with gpt-5.1-mini are missing from the token chart for the 7d range.',
  'Design a landing page that replaces the raw table with a session viewer.',
  'Write a Vitest suite for the new url parser that covers edge cases for web_searches.',
  'Can you audit our Playwright e2e for flakiness and suggest fixes?',
  'Summarize the last 20 errors in the ingestor log and group them by root cause.',
  'Build a CLI that tails the agent_turns table and prints a live throughput readout.',
  'Help me outline a blog post about the Moraine architecture, about 800 words.',
];

const ASSISTANT_TEXTS = [
  'Looking at the current pagination, it\u2019s offset-based. I\u2019ll switch the handler to cursor-based and update the client store to match.',
  'The heartbeat table shows three entries with age > 300s. Let me pull the surrounding rows to look for clustering.',
  'I\u2019ll read theme.ts first and then extract a shared createChartTheme() that both panels can call on change.',
  'Before drafting, let me enumerate the tables that need replication and confirm which have TTLs.',
  'Checking the bucket axis for 7d \u2014 it looks like the top-8 model filter is excluding gpt-5.1-mini when its share drops below 2%.',
  'I\u2019ll sketch four variations exploring layout, visual tone, and turn visualization, then we can iterate.',
  'Starting with the url parser tests \u2014 I\u2019ll cover http vs https, missing hosts, javascript: scheme, and empty strings.',
  'First I\u2019ll tally the flaky spec runs from the last 30 days, then cross-reference with recent selector changes.',
  'I can group errors by the first line of the stacktrace. Pulling the last 20 now.',
  'I\u2019ll tail with an interval poll on agent_turns and render a compact line chart in the terminal.',
  'Let me skim a few recent docs so the post reflects the current state and not what we had six months ago.',
];

const TOOL_ARG_SAMPLES: Record<string, Array<Record<string, unknown>>> = {
  read_file: [{ path: 'web/monitor/src/App.svelte' }, { path: 'web/monitor/src/lib/charts/analytics.ts' }],
  write_file: [
    { path: 'web/monitor/src/lib/session-viewer/SessionList.svelte', content: '<script lang="ts">...' },
  ],
  str_replace_edit: [
    { path: 'web/monitor/src/App.svelte', old_string: '<TablePanel ...', new_string: '<SessionPanel ...' },
  ],
  grep: [{ pattern: 'fetchTableDetail', path: 'web/monitor/src' }],
  list_files: [{ path: 'web/monitor/src/lib' }],
  run_script: [{ code: 'const r = await fetch("/api/sessions?cursor=0"); log(r.status);' }],
  web_fetch: [{ url: 'https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication' }],
  web_search: [{ query: 'svelte 5 runes migration guide' }],
  save_screenshot: [{ path: 'Moraine Monitor.html', save_path: 'screenshots/session-list.png' }],
  show_html: [{ path: 'Moraine Monitor.html' }],
};

const TOOL_RESULTS: Record<string, string> = {
  read_file: 'Read 229 lines, 7031 chars. Top-level: imports, state, hydrateFast/Slow, 8 event handlers, onMount.',
  write_file: 'Wrote 3,482 characters to web/monitor/src/lib/session-viewer/SessionList.svelte',
  str_replace_edit: 'Applied 1 edit. 0 lines added, 0 removed.',
  grep: '4 matches across 2 files.',
  list_files: 'api/  charts/  components/  constants.ts  state/  types/  utils/  view/',
  run_script: '200 OK. { "ok": true, "cursor": "eyJzZWVkIjoxfQ==", "sessions": [ ... 25 items ... ] }',
  web_fetch: 'Fetched 184 KB. Extracted 12,402 words on ReplicatedMergeTree, Keeper setup, and config.xml macros.',
  web_search: '10 results. Top: "Svelte 5 runes \u2014 official migration guide", "$state vs stores", "Migration gotchas".',
  save_screenshot: 'Captured screenshots/session-list.png (1440x900, 312 KB).',
  show_html: 'Opened Moraine Monitor.html in preview. No console errors.',
};

const HARNESSES: Harness[] = [
  { id: 'claude-code', label: 'claude-code', short: 'CC', hue: 25 },
  { id: 'codex', label: 'codex', short: 'CX', hue: 150 },
  { id: 'hermes', label: 'hermes', short: 'HM', hue: 265 },
  { id: 'cursor', label: 'cursor', short: 'CU', hue: 200 },
  { id: 'aider', label: 'aider', short: 'AD', hue: 340 },
  { id: 'cli', label: 'cli', short: 'CL', hue: 60 },
];

const TITLES = [
  'Cursor pagination for sessions API',
  'Investigate stale ingestor heartbeat at 03:00 UTC',
  'Refactor analytics panel theme plumbing',
  'Draft: single-node \u2192 replicated ClickHouse migration',
  'gpt-5.1-mini missing from 7d token chart',
  'Session viewer landing page \u2014 explorations',
  'Vitest suite for url parser edge cases',
  'Playwright flake audit \u2014 last 30 days',
  'Cluster last 20 ingestor errors by root cause',
  'CLI: live agent_turns throughput tail',
  'Moraine architecture blog draft',
  'Add dark theme parity for StatGrid',
  'Cost explorer \u2014 research',
  'Cancel handling for mid-turn aborts',
  'Type-only circular import cleanup',
];

function seeded(seed: number): () => number {
  let x = seed | 0;
  return () => {
    x = (x * 1664525 + 1013904223) | 0;
    return (x >>> 0) / 4294967296;
  };
}

function makeGenerator(seed: number) {
  const rng = seeded(seed);
  const pick = <T>(arr: T[]): T => arr[Math.floor(rng() * arr.length)];
  const randInt = (lo: number, hi: number): number => lo + Math.floor(rng() * (hi - lo + 1));

  function pickFollowup(): string {
    const followups = [
      'Now try it with the 7d range.',
      'Can you show me the diff?',
      'Looks good \u2014 commit and move on.',
      'Wait, I don\u2019t think that\u2019s right. Check the tailwind config.',
      'Keep going.',
      'Any other places this pattern shows up?',
      'Summarize what you changed so far.',
      'Revert that last change and try something different.',
      'Write a test for this before we continue.',
    ];
    return followups[Math.floor(rng() * followups.length)];
  }

  function genTurn(turnIdx: number, now: number, offsetMs: number): Turn {
    const promptIdx = randInt(0, PROMPTS.length - 1);
    const model = pick(MODELS);
    const toolCount = rng() < 0.45 ? randInt(1, 4) : rng() < 0.5 ? randInt(0, 1) : 0;
    const startedAt = now - offsetMs;
    const durationMs = randInt(1200, 42_000);
    const steps: Step[] = [];

    steps.push({
      kind: 'user',
      at: startedAt,
      text: turnIdx === 0 ? PROMPTS[promptIdx] : pickFollowup(),
    });

    const assistantChunks = randInt(1, toolCount > 0 ? 3 : 2);
    let cursor = startedAt + randInt(200, 1800);

    for (let i = 0; i < assistantChunks; i++) {
      steps.push({
        kind: 'assistant',
        at: cursor,
        text: ASSISTANT_TEXTS[randInt(0, ASSISTANT_TEXTS.length - 1)],
        tokens: randInt(60, 820),
      });
      cursor += randInt(300, 4500);

      if (i < toolCount) {
        const tool = pick(TOOLS);
        const argsList = TOOL_ARG_SAMPLES[tool] || [{}];
        const args = argsList[Math.floor(rng() * argsList.length)];
        const callAt = cursor;
        cursor += randInt(200, 6500);
        const resultAt = cursor;
        cursor += randInt(100, 2200);
        const toolCall: ToolCallStep = {
          kind: 'tool_call',
          at: callAt,
          tool,
          args,
          latencyMs: resultAt - callAt,
          result: TOOL_RESULTS[tool] || 'ok',
          resultAt,
          status: rng() < 0.08 ? 'error' : 'ok',
        };
        steps.push(toolCall);
      }
    }

    const promptTokens = randInt(300, 6400);
    const completionTokens = randInt(80, 2200);

    return {
      idx: turnIdx,
      model,
      startedAt,
      endedAt: startedAt + durationMs,
      durationMs,
      promptTokens,
      completionTokens,
      totalTokens: promptTokens + completionTokens,
      toolCalls: steps.filter((s) => s.kind === 'tool_call').length,
      steps,
    };
  }

  function genSession(i: number, now: number): Session {
    const nTurns = randInt(2, 9);
    const sessionStart = now - randInt(60_000, 72 * 3600_000);
    const harness = pick(HARNESSES);
    const tagCount = randInt(0, 3);
    const tagSet = new Set<string>();
    while (tagSet.size < tagCount) tagSet.add(pick(TAGS));

    const turns: Turn[] = [];
    let offset = now - sessionStart;
    for (let t = 0; t < nTurns; t++) {
      const turn = genTurn(t, now, offset);
      turns.push(turn);
      offset -= turn.durationMs + randInt(1200, 90_000);
      if (offset < 0) break;
    }
    turns.reverse();

    const totalTokens = turns.reduce((a, t) => a + t.totalTokens, 0);
    const totalToolCalls = turns.reduce((a, t) => a + t.toolCalls, 0);
    const modelSet = [...new Set(turns.map((t) => t.model))];
    const lastTurn = turns[turns.length - 1];
    const endedAt = lastTurn.endedAt;
    const durationMs = endedAt - sessionStart;

    const nonCompleted: SessionStatus[] = ['active', 'cancelled', 'error'];
    const status: SessionStatus = rng() < 0.78 ? 'completed' : pick(nonCompleted);

    return {
      id: 'sess_' + Math.floor(rng() * 1e12).toString(16).padStart(10, '0'),
      title: TITLES[i % TITLES.length],
      harness,
      startedAt: sessionStart,
      endedAt,
      durationMs,
      status,
      models: modelSet,
      turns,
      totalTokens,
      totalToolCalls,
      tags: [...tagSet],
      traceId: 'tr_' + Math.floor(rng() * 1e14).toString(16).padStart(12, '0'),
    };
  }

  return { genSession };
}

export function generateMockSessions(count = 24, seed = 7, now: number = Date.now()): Session[] {
  const { genSession } = makeGenerator(seed);
  return Array.from({ length: count }, (_, i) => genSession(i, now));
}

export const MOCK_MODELS = MODELS;
export const MOCK_HARNESSES = HARNESSES;

<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import StatusDot from './StatusDot.svelte';
  import Chip from './Chip.svelte';
  import TurnViz from './TurnViz.svelte';
  import SessionNodes from './SessionNodes.svelte';
  import type { Session } from '../../types/sessions';
  import { fmtDate, fmtDuration, fmtTokens } from '../../utils/sessionFormat';

  export let session: Session;
  export let layout: 'sidepanel' | 'inline' | 'split' | 'drawer' = 'sidepanel';
  export let closable = false;

  const dispatch = createEventDispatcher<{ close: void }>();

  type VizMode = 'transcript' | 'flamegraph';

  let expandedTools = new Set<string>();
  let vizMode: VizMode = 'transcript';

  let currentSessionId: string | null = null;
  $: if (session.id !== currentSessionId) {
    currentSessionId = session.id;
    expandedTools = new Set();
  }

  function toggleTool(key: string): void {
    const next = new Set(expandedTools);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    expandedTools = next;
  }

  function setMode(mode: VizMode): void {
    vizMode = mode;
  }
</script>

<div class="mv-detail mv-detail-{layout}">
  <div class="mv-detail-head">
    <div class="mv-detail-titlerow">
      <StatusDot status={session.status} endedAt={session.endedAt} />
      <h3 class="mv-detail-title">{session.title}</h3>
      <div class="mv-viz-toggle" role="group" aria-label="Detail view">
        <button
          type="button"
          class:is-active={vizMode === 'transcript'}
          aria-pressed={vizMode === 'transcript'}
          on:click={() => setMode('transcript')}
        >
          transcript
        </button>
        <button
          type="button"
          class:is-active={vizMode === 'flamegraph'}
          aria-pressed={vizMode === 'flamegraph'}
          on:click={() => setMode('flamegraph')}
        >
          flamegraph
        </button>
      </div>
      {#if closable}
        <button class="mv-iconbtn" type="button" aria-label="Close session detail" on:click={() => dispatch('close')}>✕</button>
      {/if}
    </div>
    <div class="mv-detail-metagrid">
      <div>
        <div class="mv-meta-k">session id</div>
        <div class="mono mv-meta-v">{session.id}</div>
      </div>
      <div>
        <div class="mv-meta-k">trace id</div>
        <div class="mono mv-meta-v">{session.traceId}</div>
      </div>
      <div>
        <div class="mv-meta-k">started</div>
        <div class="mv-meta-v">{fmtDate(session.startedAt)}</div>
      </div>
      <div>
        <div class="mv-meta-k">duration</div>
        <div class="mono mv-meta-v">{fmtDuration(session.durationMs)}</div>
      </div>
      <div>
        <div class="mv-meta-k">turns</div>
        <div class="mono mv-meta-v">{session.turns.length}</div>
      </div>
      <div>
        <div class="mv-meta-k">tokens</div>
        <div class="mono mv-meta-v">{session.totalTokens.toLocaleString()}</div>
      </div>
      <div>
        <div class="mv-meta-k">tool calls</div>
        <div class="mono mv-meta-v">{session.totalToolCalls}</div>
      </div>
      <div>
        <div class="mv-meta-k">models</div>
        <div class="mv-meta-v">
          {#each session.models as model (model)}
            <Chip>{model}</Chip>
          {/each}
        </div>
      </div>
    </div>
  </div>

  <div class="mv-turns">
    {#if vizMode === 'transcript'}
      <SessionNodes {session} {expandedTools} on:toggleTool={(e) => toggleTool(e.detail)} />
    {:else}
      <div class="mv-flame">
        {#each session.turns as turn, i (turn.idx)}
          {@const userStep = turn.steps.find((s) => s.kind === 'user')}
          <section class="mv-flame-turn">
            {#if userStep && userStep.kind === 'user'}
              <div class="mv-flame-prompt">
                <div class="mv-flame-prompt-head mono">
                  <span class="mv-flame-prompt-label">user</span>
                  <span class="mv-flame-prompt-meta">
                    turn {String(i + 1).padStart(2, '0')} · {turn.model || '—'} · {fmtDuration(turn.durationMs)} · {fmtTokens(turn.totalTokens)} tok · {turn.toolCalls} tool{turn.toolCalls === 1 ? '' : 's'}
                  </span>
                </div>
                <div class="mv-flame-prompt-text">{userStep.text}</div>
              </div>
            {/if}
            <TurnViz
              {turn}
              variant="trace"
              hideUserSteps
              {expandedTools}
              on:toggleTool={(e) => toggleTool(e.detail)}
            />
          </section>
        {/each}
      </div>
    {/if}
  </div>
</div>

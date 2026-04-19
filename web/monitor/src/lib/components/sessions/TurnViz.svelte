<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { Turn, TurnVizVariant, Step } from '../../types/sessions';
  import { fmtClock, fmtDuration, summarizeArgs } from '../../utils/sessionFormat';

  export let turn: Turn;
  export let variant: TurnVizVariant = 'chat';
  export let expandedTools: Set<string> = new Set();
  export let hideUserSteps = false;

  $: steps = hideUserSteps ? turn.steps.filter((s) => s.kind !== 'user') : turn.steps;

  const dispatch = createEventDispatcher<{ toggleTool: string }>();

  function keyFor(step: Step, i: number): string {
    return `${turn.idx}:${i}`;
  }

  function hasText(step: Step): step is Extract<Step, { text: string }> {
    return 'text' in step && typeof step.text === 'string';
  }

  $: traceBounds = computeTraceBounds(turn);

  function computeTraceBounds(t: Turn): { start: number; span: number } {
    const start = t.startedAt;
    const ends = t.steps.map((s) => (s.kind === 'tool_call' ? s.resultAt : s.at));
    const end = Math.max(t.endedAt, ...ends);
    return { start, span: Math.max(1, end - start) };
  }

  const axisPositions = [0, 0.25, 0.5, 0.75, 1];
</script>

{#if variant === 'timeline'}
  <div class="mv-turn mv-turn-timeline">
    <div class="mv-tl-spine"></div>
    {#each turn.steps as step, i (keyFor(step, i))}
      {@const key = keyFor(step, i)}
      {#if step.kind === 'tool_call'}
        {@const open = expandedTools.has(key)}
        <div class="mv-tl-node mv-tl-tool_call" class:is-error={step.status === 'error'}>
          <div class="mv-tl-dot"></div>
          <div class="mv-tl-time mono">{fmtClock(step.at)}</div>
          <button
            type="button"
            class="mv-tl-body mv-tl-body-btn"
            aria-expanded={open}
            on:click={() => dispatch('toggleTool', key)}
          >
            <div class="mv-tl-headrow">
              <span class="mv-tl-kind">tool</span>
              <span class="mv-toolcall-name mono">{step.tool}</span>
              <span class="mv-toolcall-latency mono">{step.latencyMs}ms</span>
            </div>
            <div class="mv-toolcall-arg mono">{summarizeArgs(step.args)}</div>
            {#if open}
              <div class="mv-toolcall-body mv-toolcall-body-inline">
                <div class="mv-tc-section">
                  <div class="mv-tc-label">arguments</div>
                  <pre class="mv-tc-code">{JSON.stringify(step.args, null, 2)}</pre>
                </div>
                <div class="mv-tc-section">
                  <div class="mv-tc-label">result</div>
                  <pre class="mv-tc-code">{step.result}</pre>
                </div>
              </div>
            {/if}
          </button>
        </div>
      {:else}
        <div class="mv-tl-node mv-tl-{step.kind}">
          <div class="mv-tl-dot"></div>
          <div class="mv-tl-time mono">{fmtClock(step.at)}</div>
          <div class="mv-tl-body">
            <div class="mv-tl-headrow">
              <span class="mv-tl-kind">{step.kind === 'user' ? 'user' : 'assistant'}</span>
              {#if step.kind === 'assistant'}
                <span class="mono mv-tl-model">{turn.model}</span>
              {/if}
            </div>
            <div class="mv-tl-text">{hasText(step) ? step.text : ''}</div>
          </div>
        </div>
      {/if}
    {/each}
  </div>
{:else if variant === 'trace'}
  <div class="mv-turn mv-turn-trace">
    <div class="mv-tr-axis">
      {#each axisPositions as p, i (i)}
        <div class="mv-tr-tick" style="left: {p * 100}%">
          <span class="mono">{fmtDuration(Math.round(traceBounds.span * p))}</span>
        </div>
      {/each}
    </div>
    {#each steps as step, i (keyFor(step, i))}
      {@const key = keyFor(step, i)}
      {@const at = step.at}
      {@const endAt = step.kind === 'tool_call' ? step.resultAt : step.at + 600}
      {@const leftPct = ((at - traceBounds.start) / traceBounds.span) * 100}
      {@const widthPct = Math.max(1, ((endAt - at) / traceBounds.span) * 100)}
      {@const expanded = expandedTools.has(key)}
      {@const label = step.kind === 'tool_call' ? step.tool : step.kind === 'user' ? 'user' : 'assistant'}
      <div class="mv-tr-row mv-tr-{step.kind}">
        <div class="mv-tr-label mono">{label}</div>
        <div class="mv-tr-track">
          <button
            type="button"
            class="mv-tr-bar mv-tr-bar-{step.kind}"
            class:is-error={step.kind === 'tool_call' && step.status === 'error'}
            style="left: {leftPct}%; width: {widthPct}%"
            on:click={() => step.kind === 'tool_call' && dispatch('toggleTool', key)}
          >
            <span class="mv-tr-bar-text mono">
              {#if step.kind === 'tool_call'}
                {step.tool} · {step.latencyMs}ms
              {:else if step.kind === 'assistant' && step.tokens}
                {step.kind} · {step.tokens} tok
              {:else}
                {step.kind}
              {/if}
            </span>
          </button>
        </div>
        {#if step.kind === 'tool_call' && expanded}
          <div class="mv-tr-expand">
            <div class="mv-tc-section">
              <div class="mv-tc-label">arguments</div>
              <pre class="mv-tc-code">{JSON.stringify(step.args, null, 2)}</pre>
            </div>
            <div class="mv-tc-section">
              <div class="mv-tc-label">result</div>
              <pre class="mv-tc-code">{step.result}</pre>
            </div>
          </div>
        {:else if step.kind !== 'tool_call'}
          <div class="mv-tr-text">{hasText(step) ? step.text : ''}</div>
        {/if}
      </div>
    {/each}
  </div>
{:else if variant === 'document'}
  <div class="mv-turn mv-turn-document">
    {#each turn.steps as step, i (keyFor(step, i))}
      {@const key = keyFor(step, i)}
      {#if step.kind === 'tool_call'}
        {@const open = expandedTools.has(key)}
        <div class="mv-doc-tool">
          <button type="button" class="mv-doc-tool-head" on:click={() => dispatch('toggleTool', key)}>
            <span class="mv-doc-marker">§</span>
            <span class="mono mv-doc-tool-name">{step.tool}</span>
            <span class="mv-doc-tool-summary mono">{summarizeArgs(step.args)}</span>
            <span class="mv-doc-tool-latency mono">{step.latencyMs}ms</span>
          </button>
          {#if open}
            <div class="mv-doc-tool-body">
              <div class="mv-tc-section">
                <div class="mv-tc-label">arguments</div>
                <pre class="mv-tc-code">{JSON.stringify(step.args, null, 2)}</pre>
              </div>
              <div class="mv-tc-section">
                <div class="mv-tc-label">result</div>
                <pre class="mv-tc-code">{step.result}</pre>
              </div>
            </div>
          {/if}
        </div>
      {:else}
        <div class="mv-doc-line mv-doc-line-{step.kind}">
          <span class="mv-doc-speaker">{step.kind === 'user' ? 'U' : 'A'}</span>
          <span class="mv-doc-text">{hasText(step) ? step.text : ''}</span>
        </div>
      {/if}
    {/each}
  </div>
{:else}
  <div class="mv-turn mv-turn-chat">
    {#each turn.steps as step, i (keyFor(step, i))}
      {@const key = keyFor(step, i)}
      {#if step.kind === 'user'}
        <div class="mv-bubble mv-bubble-user">
          <div class="mv-bubble-role">User</div>
          <div class="mv-bubble-text">{step.text}</div>
        </div>
      {:else if step.kind === 'assistant'}
        <div class="mv-bubble mv-bubble-assistant">
          <div class="mv-bubble-role">
            Assistant <span class="mv-bubble-meta">{turn.model}</span>
          </div>
          <div class="mv-bubble-text">{step.text}</div>
        </div>
      {:else if step.kind === 'tool_call'}
        {@const open = expandedTools.has(key)}
        <div class="mv-toolcall" class:is-error={step.status === 'error'}>
          <button
            type="button"
            class="mv-toolcall-head"
            aria-expanded={open}
            on:click={() => dispatch('toggleTool', key)}
          >
            <span class="mv-tri" data-open={open ? 'true' : 'false'}>▸</span>
            <span class="mv-toolcall-name mono">{step.tool}</span>
            <span class="mv-toolcall-arg mono">{summarizeArgs(step.args)}</span>
            <span class="mv-toolcall-latency mono">{step.latencyMs}ms</span>
            {#if step.status === 'error'}
              <span class="mv-toolcall-err">error</span>
            {/if}
          </button>
          {#if open}
            <div class="mv-toolcall-body">
              <div class="mv-tc-section">
                <div class="mv-tc-label">arguments</div>
                <pre class="mv-tc-code">{JSON.stringify(step.args, null, 2)}</pre>
              </div>
              <div class="mv-tc-section">
                <div class="mv-tc-label">result</div>
                <pre class="mv-tc-code">{step.result}</pre>
              </div>
            </div>
          {/if}
        </div>
      {/if}
    {/each}
  </div>
{/if}

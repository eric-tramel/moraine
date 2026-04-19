<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { Session, Step, Turn } from '../../types/sessions';
  import { fmtClock, fmtDuration, fmtTokens, summarizeArgs } from '../../utils/sessionFormat';

  export let session: Session;
  export let expandedTools: Set<string> = new Set();

  type BoundaryNode = { kind: 'turn_boundary'; turnIdx: number; at: number; turn: Turn };
  type StepNode = Step & { key: string; turnIdx: number };
  type Node = BoundaryNode | StepNode;

  const dispatch = createEventDispatcher<{ toggleTool: string }>();

  $: nodes = buildNodes(session);

  function buildNodes(s: Session): Node[] {
    const out: Node[] = [];
    s.turns.forEach((turn, ti) => {
      if (ti > 0) {
        out.push({ kind: 'turn_boundary', turnIdx: ti, at: turn.startedAt, turn });
      }
      turn.steps.forEach((step, si) => {
        out.push({ ...step, key: `${ti}:${si}`, turnIdx: ti });
      });
    });
    return out;
  }
</script>

<div class="mv-nodes">
  <div class="mv-nodes-spine" aria-hidden="true"></div>
  {#each nodes as node, i (i)}
    {#if node.kind === 'turn_boundary'}
      <div class="mv-node mv-node-boundary">
        <span class="mv-node-time mono">{fmtClock(node.at)}</span>
        <span class="mv-node-dot"></span>
        <div class="mv-node-body">
          <span class="mv-node-label mono">turn {String(node.turnIdx + 1).padStart(2, '0')}</span>
          <span class="mv-node-meta mono">
            {node.turn.model} · {fmtDuration(node.turn.durationMs)} · {fmtTokens(node.turn.totalTokens)} tok
          </span>
        </div>
      </div>
    {:else if node.kind === 'user'}
      <div class="mv-node mv-node-user">
        <span class="mv-node-time mono">{fmtClock(node.at)}</span>
        <span class="mv-node-dot"></span>
        <div class="mv-node-body">
          <span class="mv-node-label mono">user</span>
          <span class="mv-node-text">{node.text}</span>
        </div>
      </div>
    {:else if node.kind === 'assistant'}
      <div class="mv-node mv-node-assistant">
        <span class="mv-node-time mono">{fmtClock(node.at)}</span>
        <span class="mv-node-dot"></span>
        <div class="mv-node-body">
          <span class="mv-node-label mono">assistant</span>
          <span class="mv-node-text">{node.text}</span>
        </div>
      </div>
    {:else if node.kind === 'tool_call'}
      {@const expanded = expandedTools.has(node.key)}
      {@const err = node.status === 'error'}
      <div
        class="mv-node mv-node-tool"
        class:is-error={err}
        class:is-open={expanded}
      >
        <span class="mv-node-time mono">{fmtClock(node.at)}</span>
        <span class="mv-node-dot"></span>
        <div class="mv-node-body">
          <button
            type="button"
            class="mv-node-toolhead"
            aria-expanded={expanded}
            on:click={() => dispatch('toggleTool', node.key)}
          >
            <span class="mv-node-label mono">tool</span>
            <span class="mv-node-toolname mono">{node.tool}</span>
            <span class="mv-node-toolarg mono">{summarizeArgs(node.args)}</span>
            <span class="mv-node-toollat mono">{node.latencyMs}ms</span>
            {#if err}
              <span class="mv-node-err">error</span>
            {/if}
            <span class="mv-node-tri" data-open={expanded ? 'true' : 'false'}>▸</span>
          </button>
          {#if expanded}
            <div class="mv-node-toolbody">
              <div class="mv-tc-section">
                <div class="mv-tc-label">arguments</div>
                <pre class="mv-tc-code">{JSON.stringify(node.args, null, 2)}</pre>
              </div>
              <div class="mv-tc-section">
                <div class="mv-tc-label">result</div>
                <pre class="mv-tc-code">{node.result}</pre>
              </div>
            </div>
          {/if}
        </div>
      </div>
    {/if}
  {/each}
</div>

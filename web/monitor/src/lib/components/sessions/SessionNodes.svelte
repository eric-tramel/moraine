<script lang="ts">
  import type { SessionTurn } from '../../types/sessions';
  import { fmtClock, fmtDuration } from '../../utils/sessionFormat';

  /**
   * The turn spine of an opened session.
   *
   * Each node is one turn as the canonical reader returns it: bounded user and
   * final-response summaries, the tools it called, and its counts. There is no
   * per-step rendering because there is no per-step read — the reader pages
   * turns, and the summaries below are what it carries.
   */
  export let turns: SessionTurn[] = [];
</script>

<div class="mv-nodes">
  <div class="mv-nodes-spine" aria-hidden="true"></div>
  {#each turns as turn (turn.turnSeq)}
    <div class="mv-node mv-node-boundary">
      <span class="mv-node-time mono">{fmtClock(turn.startedAt)}</span>
      <span class="mv-node-dot"></span>
      <div class="mv-node-body">
        <span class="mv-node-label mono">turn {String(turn.turnSeq).padStart(2, '0')}</span>
        <span class="mv-node-meta mono">
          {fmtDuration(Math.max(0, turn.endedAt - turn.startedAt))} · {turn.eventCount} events · {turn.toolCalls}
          tool{turn.toolCalls === 1 ? '' : 's'}{turn.completed ? '' : ' · in progress'}
        </span>
      </div>
    </div>
    {#if turn.userInput}
      <div class="mv-node mv-node-user">
        <span class="mv-node-time mono">{fmtClock(turn.startedAt)}</span>
        <span class="mv-node-dot"></span>
        <div class="mv-node-body">
          <span class="mv-node-label mono">user</span>
          <span class="mv-node-text">{turn.userInput}</span>
        </div>
      </div>
    {/if}
    {#if turn.toolsCalled.length > 0}
      <div class="mv-node mv-node-tool">
        <span class="mv-node-time mono">—</span>
        <span class="mv-node-dot"></span>
        <div class="mv-node-body">
          <span class="mv-node-label mono">tools</span>
          <span class="mv-node-text mono">{turn.toolsCalled.join(', ')}</span>
        </div>
      </div>
    {/if}
    {#if turn.finalResponse}
      <div class="mv-node mv-node-assistant">
        <span class="mv-node-time mono">{fmtClock(turn.endedAt)}</span>
        <span class="mv-node-dot"></span>
        <div class="mv-node-body">
          <span class="mv-node-label mono">assistant</span>
          <span class="mv-node-text">{turn.finalResponse}</span>
        </div>
      </div>
    {/if}
  {/each}
</div>

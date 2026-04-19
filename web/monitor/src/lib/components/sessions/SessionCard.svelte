<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import StatusDot from './StatusDot.svelte';
  import HarnessBadge from './HarnessBadge.svelte';
  import Chip from './Chip.svelte';
  import type { Session } from '../../types/sessions';
  import { fmtDuration, fmtRelative, fmtTokens } from '../../utils/sessionFormat';

  export let session: Session;
  export let active = false;
  export let variant: 'library' | 'timeline' = 'library';

  const dispatch = createEventDispatcher<{ open: Session }>();

  $: firstPrompt = findFirstPrompt(session);

  function findFirstPrompt(s: Session): string {
    const firstTurn = s.turns[0];
    if (!firstTurn) return '';
    const userStep = firstTurn.steps.find((step) => step.kind === 'user');
    return userStep && userStep.kind === 'user' ? userStep.text : '';
  }
</script>

<button
  type="button"
  class="mv-card mv-card-{variant}"
  class:is-active={active}
  on:click={() => dispatch('open', session)}
>
  <div class="mv-card-head">
    <div class="mv-card-title-row">
      <StatusDot status={session.status} endedAt={session.endedAt} />
      <span class="mv-card-title">{session.title}</span>
    </div>
    <span class="mv-card-time">{fmtRelative(session.endedAt)}</span>
  </div>
  <div class="mv-card-prompt">{firstPrompt}</div>
  <div class="mv-card-meta">
    <span class="mv-meta-item">
      <HarnessBadge harness={session.harness} size={18} />
      {session.harness.label}
    </span>
    <span class="mv-meta-sep">·</span>
    <span class="mv-meta-item mono">{session.turns.length} turns</span>
    <span class="mv-meta-sep">·</span>
    <span class="mv-meta-item mono">{fmtTokens(session.totalTokens)} tok</span>
    <span class="mv-meta-sep">·</span>
    <span class="mv-meta-item mono">{session.totalToolCalls} tools</span>
    <span class="mv-meta-sep">·</span>
    <span class="mv-meta-item mono">{fmtDuration(session.durationMs)}</span>
    {#if session.models.length > 0}
      <span class="mv-meta-sep">·</span>
      <span class="mv-meta-item">
        {#each session.models as model (model)}
          <Chip>{model}</Chip>
        {/each}
      </span>
    {/if}
  </div>
</button>

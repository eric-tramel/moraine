<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import StatusDot from './StatusDot.svelte';
  import HarnessBadge from './HarnessBadge.svelte';
  import Chip from './Chip.svelte';
  import type { SessionSummary } from '../../types/sessions';
  import { fmtDuration, fmtRelative } from '../../utils/sessionFormat';
  import { harnessDescriptor } from '../../utils/harness';

  export let session: SessionSummary;
  export let active = false;
  export let variant: 'library' | 'timeline' = 'library';

  const dispatch = createEventDispatcher<{ open: SessionSummary }>();

  // The server renders the same label ladder MCP uses (title -> summary -> slug
  // -> synthesized), so a card reads identically on both surfaces. The card
  // never reaches into transcript content for a preview line: the feed carries
  // none, by design.
  $: harness = harnessDescriptor(session.harness);
  $: subtitle = session.sessionSummary ?? '';
  $: durationMs = Math.max(0, session.endedAt - session.startedAt);
</script>

<button
  type="button"
  class="mv-card mv-card-{variant}"
  class:is-active={active}
  on:click={() => dispatch('open', session)}
>
  <div class="mv-card-head">
    <div class="mv-card-title-row">
      <StatusDot endedAt={session.endedAt} />
      <span class="mv-card-title">{session.displayLabel}</span>
    </div>
    <span class="mv-card-time">{fmtRelative(session.endedAt)}</span>
  </div>
  <div class="mv-card-prompt">{subtitle}</div>
  <div class="mv-card-meta">
    <span class="mv-meta-item">
      <HarnessBadge {harness} size={18} />
      {harness.label}
    </span>
    <span class="mv-meta-sep">·</span>
    <span class="mv-meta-item mono">{session.turnCount} turns</span>
    <span class="mv-meta-sep">·</span>
    <span class="mv-meta-item mono">{session.eventCount} events</span>
    <span class="mv-meta-sep">·</span>
    <span class="mv-meta-item mono">{session.toolCallCount} tools</span>
    <span class="mv-meta-sep">·</span>
    <span class="mv-meta-item mono">{fmtDuration(durationMs)}</span>
    {#if session.inferenceProvider}
      <span class="mv-meta-sep">·</span>
      <span class="mv-meta-item"><Chip>{session.inferenceProvider}</Chip></span>
    {/if}
  </div>
</button>

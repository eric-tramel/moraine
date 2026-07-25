<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import StatusDot from './StatusDot.svelte';
  import Chip from './Chip.svelte';
  import SessionNodes from './SessionNodes.svelte';
  import type { SessionSummary, SessionTranscript } from '../../types/sessions';
  import { fmtDate, fmtDuration } from '../../utils/sessionFormat';

  /**
   * An opened session: the summary the list already had, progressively filled
   * with turns from `/api/v1/sessions/:id/page`.
   *
   * `transcript` is null until the first page lands. Header facts come from the
   * transcript once it does, and from the summary before that, so the panel is
   * useful immediately rather than blank while a page is in flight.
   */
  export let summary: SessionSummary;
  export let transcript: SessionTranscript | null = null;
  export let loading = false;
  export let loadingMore = false;
  export let hasMore = false;
  export let errorMessage: string | null = null;
  export let layout: 'sidepanel' | 'inline' | 'split' | 'drawer' = 'sidepanel';
  export let closable = false;

  const dispatch = createEventDispatcher<{ close: void; loadMore: void }>();

  $: turns = transcript?.turns ?? [];
  $: startedAt = transcript?.startedAt ?? summary.startedAt;
  $: endedAt = transcript?.endedAt ?? summary.endedAt;
  $: turnCount = transcript?.turnCount ?? summary.turnCount;
  $: eventCount = transcript?.eventCount ?? summary.eventCount;
</script>

<div class="mv-detail mv-detail-{layout}">
  <div class="mv-detail-head">
    <div class="mv-detail-titlerow">
      <StatusDot {endedAt} />
      <h3 class="mv-detail-title">{summary.displayLabel}</h3>
      {#if closable}
        <button class="mv-iconbtn" type="button" aria-label="Close session detail" on:click={() => dispatch('close')}>✕</button>
      {/if}
    </div>
    <div class="mv-detail-metagrid">
      <div>
        <div class="mv-meta-k">session id</div>
        <div class="mono mv-meta-v">{summary.id}</div>
      </div>
      <div>
        <div class="mv-meta-k">harness</div>
        <div class="mv-meta-v">{summary.harness ?? '—'}</div>
      </div>
      <div>
        <div class="mv-meta-k">source</div>
        <div class="mv-meta-v">{summary.source ?? '—'}</div>
      </div>
      <div>
        <div class="mv-meta-k">started</div>
        <div class="mv-meta-v">{fmtDate(startedAt)}</div>
      </div>
      <div>
        <div class="mv-meta-k">duration</div>
        <div class="mono mv-meta-v">{fmtDuration(Math.max(0, endedAt - startedAt))}</div>
      </div>
      <div>
        <div class="mv-meta-k">turns</div>
        <div class="mono mv-meta-v">{turnCount}</div>
      </div>
      <div>
        <div class="mv-meta-k">events</div>
        <div class="mono mv-meta-v">{eventCount}</div>
      </div>
      <div>
        <div class="mv-meta-k">mode</div>
        <div class="mv-meta-v">
          <Chip>{summary.mode}</Chip>
          {#if summary.inferenceProvider}
            <Chip>{summary.inferenceProvider}</Chip>
          {/if}
        </div>
      </div>
    </div>
  </div>

  <div class="mv-turns">
    {#if errorMessage}
      <div class="mv-empty" role="status" aria-live="polite">{errorMessage}</div>
    {:else if loading && turns.length === 0}
      <div class="mv-empty">Loading turns…</div>
    {:else if turns.length === 0}
      <div class="mv-empty">This session has no turns to show.</div>
    {:else}
      <SessionNodes {turns} />
      <div class="mv-loadmore">
        <span class="mv-loadmore-count mono">{turns.length} of {turnCount} turns loaded</span>
        {#if hasMore}
          <button
            class="mv-loadmore-btn"
            type="button"
            disabled={loadingMore}
            on:click={() => dispatch('loadMore')}
          >
            {loadingMore ? 'Loading…' : 'Load more turns'}
          </button>
        {/if}
      </div>
    {/if}
  </div>
</div>

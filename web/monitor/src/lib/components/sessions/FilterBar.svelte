<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { sessionCountLabel } from '../../state/sessions';
  import type { SessionsFilter } from '../../types/sessions';

  export let filter: SessionsFilter;
  /** The harness vocabulary served by `/api/v1/status`, not scraped from the loaded page. */
  export let harnesses: string[] = [];
  /** Rows on screen, i.e. AFTER the client-side `status` narrowing. */
  export let count = 0;
  /**
   * Rows the SERVER's ranking answered with, before local narrowing; `null`
   * when no search is in effect.
   *
   * This is the population `truncated` / `hitsTruncated` / `incomplete` /
   * `dropped` all describe, and it is why the count and the qualifier cannot be
   * derived from the same number.
   */
  export let resultCount: number | null = null;
  export let searchLoading = false;
  /** A search was attempted and did not answer. Nothing may be counted. */
  export let searchFailed = false;
  /** SESSION grain: more ranked sessions existed than the server returned. */
  export let truncated = false;
  /** HIT grain: the ranking filled its event-hit budget. */
  export let hitsTruncated = false;
  /** The ranking's bounded candidate window was exhausted first (#597 §1.6). */
  export let incomplete = false;
  /** The server's exact re-check removed ranked sessions and did not refill. */
  export let dropped = false;

  // The server derives exactly these two (`completed` flag, else the activity
  // window). `cancelled` and `error` were offered here and have never been
  // producible, so selecting them queried for nothing.
  const statuses = ['all', 'active', 'completed'];

  const dispatch = createEventDispatcher<{ change: SessionsFilter }>();

  function update(next: Partial<SessionsFilter>): void {
    dispatch('change', { ...filter, ...next });
  }

  // The label is a pure function in `state/sessions.ts`, not an expression
  // here: it has to reason about two different populations (rows on screen vs
  // the server's answer) and that decision deserves a test of its own.
  $: countLabel = sessionCountLabel({
    rendered: count,
    serverResults: resultCount,
    searchLoading,
    searchFailed,
    truncated,
    hitsTruncated,
    incomplete,
    dropped,
  });
</script>

<div class="mv-filterbar">
  <div class="mv-search">
    <span class="mv-search-icon" aria-hidden="true">⌕</span>
    <!-- Whole-corpus search (issue-599 WI-09): the server ranks message content
         across every session this backend serves, so the label no longer has to
         confess to being page-local. -->
    <input
      class="mv-search-input"
      placeholder="Search all sessions by content"
      aria-label="Search all sessions by content"
      value={filter.query}
      on:input={(e) => update({ query: e.currentTarget.value })}
    />
    {#if filter.query}
      <button class="mv-search-clear" type="button" aria-label="Clear search" on:click={() => update({ query: '' })}>
        ×
      </button>
    {/if}
  </div>
  <div class="mv-filters">
    <label class="mv-filter">
      <span class="mv-filter-k">status</span>
      <select class="mv-select" value={filter.status} on:change={(e) => update({ status: e.currentTarget.value })}>
        {#each statuses as option (option)}
          <option value={option}>{option}</option>
        {/each}
      </select>
    </label>
    {#if harnesses.length > 0}
      <label class="mv-filter">
        <span class="mv-filter-k">harness</span>
        <select
          class="mv-select"
          value={filter.harness}
          on:change={(e) => update({ harness: e.currentTarget.value })}
        >
          <option value="all">all</option>
          {#each harnesses as harness (harness)}
            <option value={harness}>{harness}</option>
          {/each}
        </select>
      </label>
    {/if}
    <span class="mv-filter-count mono">{countLabel}</span>
  </div>
</div>

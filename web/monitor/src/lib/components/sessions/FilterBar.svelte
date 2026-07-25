<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { SessionsFilter } from '../../types/sessions';

  export let filter: SessionsFilter;
  /** The harness vocabulary served by `/api/v1/status`, not scraped from the loaded page. */
  export let harnesses: string[] = [];
  export let count = 0;

  // The server derives exactly these two (`completed` flag, else the activity
  // window). `cancelled` and `error` were offered here and have never been
  // producible, so selecting them queried for nothing.
  const statuses = ['all', 'active', 'completed'];

  const dispatch = createEventDispatcher<{ change: SessionsFilter }>();

  function update(next: Partial<SessionsFilter>): void {
    dispatch('change', { ...filter, ...next });
  }
</script>

<div class="mv-filterbar">
  <div class="mv-search">
    <span class="mv-search-icon" aria-hidden="true">⌕</span>
    <!-- Honest label: the feed carries no message content, so this narrows the
         loaded pages by label and id. Whole-corpus search arrives with #597. -->
    <input
      class="mv-search-input"
      placeholder="Filter loaded sessions by title or id"
      aria-label="Filter loaded sessions by title or id"
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
    <!-- "loaded", never "{count} / {total}": the feed is paged and reports no
         corpus total, so a ratio here would invent a denominator. -->
    <span class="mv-filter-count mono">{count} loaded</span>
  </div>
</div>

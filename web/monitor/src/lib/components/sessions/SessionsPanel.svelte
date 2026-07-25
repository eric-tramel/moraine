<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import FilterBar from './FilterBar.svelte';
  import V1Library from './variations/V1Library.svelte';
  import type { SessionSummary, SessionsFilter } from '../../types/sessions';

  export let sessions: SessionSummary[] = [];
  export let filtered: SessionSummary[] = [];
  export let filter: SessionsFilter = { query: '', status: 'all', harness: 'all' };
  export let harnesses: string[] = [];
  export let loading = false;
  export let loadingMore = false;
  export let hasMore = false;
  export let errorMessage: string | null = null;

  const dispatch = createEventDispatcher<{ filterChange: SessionsFilter; loadMore: void }>();

  function handleFilter(next: SessionsFilter): void {
    dispatch('filterChange', next);
  }
</script>

<section class="panel mv-root" id="sessionsPanel">
  <div class="mv-section-head">
    <div class="mv-section-title">
      <h2>Sessions</h2>
      <span class="mv-section-subtitle">Search, inspect, and replay agent sessions.</span>
    </div>
  </div>

  {#if errorMessage}
    <div class="mv-empty" role="status" aria-live="polite">{errorMessage}</div>
  {/if}

  <FilterBar {filter} {harnesses} count={filtered.length} on:change={(e) => handleFilter(e.detail)} />

  {#if loading && sessions.length === 0}
    <div class="mv-empty">Loading sessions…</div>
  {:else}
    <V1Library sessions={filtered} />
    <!-- The feed reports whether more exist; without this affordance a loaded
         page would read as the whole corpus. An empty page with `hasMore` is a
         legal "keep paging" signal, so the button appears even with no rows. -->
    {#if hasMore}
      <div class="mv-loadmore">
        <button
          class="mv-loadmore-btn"
          type="button"
          disabled={loadingMore}
          on:click={() => dispatch('loadMore')}
        >
          {loadingMore ? 'Loading…' : 'Load more sessions'}
        </button>
      </div>
    {/if}
  {/if}
</section>

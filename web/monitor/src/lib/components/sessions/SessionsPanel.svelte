<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import FilterBar from './FilterBar.svelte';
  import V1Library from './variations/V1Library.svelte';
  import { idleSessionSearch, type SessionSearchState } from '../../state/sessions';
  import type { SessionSummary, SessionsFilter } from '../../types/sessions';

  export let sessions: SessionSummary[] = [];
  export let filtered: SessionSummary[] = [];
  export let filter: SessionsFilter = { query: '', status: 'all', harness: 'all' };
  export let harnesses: string[] = [];
  export let loading = false;
  export let loadingMore = false;
  export let hasMore = false;
  export let errorMessage: string | null = null;
  export let search: SessionSearchState = idleSessionSearch;

  const dispatch = createEventDispatcher<{ filterChange: SessionsFilter; loadMore: void }>();

  function handleFilter(next: SessionsFilter): void {
    dispatch('filterChange', next);
  }

  /** A search is in effect exactly when the server has answered one. */
  $: searching = search.results !== null;
  /**
   * A search was attempted and did not answer.
   *
   * This is its own render state, not "a search that returned nothing": the
   * corpus was never searched, so neither the ranked-results empty state nor
   * the feed's own empty state may be shown, and no result count may be
   * printed. Both would assert something about the corpus that this request
   * never established.
   */
  $: searchFailed = search.error !== null;
  // "Load more" pages the time-ordered feed, and its cursor belongs to that
  // query. A ranking is bounded rather than paged, so the affordance would
  // answer a different question; `truncated` is what tells the reader a ranking
  // was cut off.
  $: showLoadMore = hasMore && !searching && !search.loading && !searchFailed;
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

  <!-- `count` is what is on screen; `resultCount` is what the server answered
       with. The bounded-answer flags describe the second, so the bar needs both
       or it attaches a server fact to a locally narrowed number. -->
  <FilterBar
    {filter}
    {harnesses}
    count={filtered.length}
    resultCount={search.results?.length ?? null}
    searchLoading={search.loading}
    {searchFailed}
    truncated={search.truncated}
    hitsTruncated={search.hitsTruncated}
    incomplete={search.incomplete}
    dropped={search.dropped}
    on:change={(e) => handleFilter(e.detail)}
  />

  {#if searchFailed}
    <!-- The dedicated failure state. It replaces the list rather than sitting
         above it: a rendered list beside "Search unavailable" reads as the
         search's answer, and the feed's rows are not that. -->
    <div class="mv-empty mv-search-failure" role="status" aria-live="polite">
      {search.error}
    </div>
  {:else if loading && sessions.length === 0 && !searching}
    <div class="mv-empty">Loading sessions…</div>
  {:else if search.loading && !searching}
    <div class="mv-empty">Searching all sessions…</div>
  {:else}
    <V1Library sessions={filtered} {searching} />
    <!-- The feed reports whether more exist; without this affordance a loaded
         page would read as the whole corpus. An empty page with `hasMore` is a
         legal "keep paging" signal, so the button appears even with no rows. -->
    {#if showLoadMore}
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

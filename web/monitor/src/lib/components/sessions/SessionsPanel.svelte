<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import FilterBar from './FilterBar.svelte';
  import V1Library from './variations/V1Library.svelte';
  import type { Harness, Session, SessionsFilter } from '../../types/sessions';

  export let sessions: Session[] = [];
  export let filtered: Session[] = [];
  export let filter: SessionsFilter = { query: '', model: 'all', status: 'all', harness: 'all' };
  export let models: string[] = [];
  export let harnesses: Harness[] = [];
  export let loading = false;
  export let errorMessage: string | null = null;

  const dispatch = createEventDispatcher<{ filterChange: SessionsFilter }>();

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

  <FilterBar
    {filter}
    {models}
    {harnesses}
    count={filtered.length}
    total={sessions.length}
    on:change={(e) => handleFilter(e.detail)}
  />

  {#if loading && sessions.length === 0}
    <div class="mv-empty">Loading sessions…</div>
  {:else}
    <V1Library sessions={filtered} />
  {/if}
</section>

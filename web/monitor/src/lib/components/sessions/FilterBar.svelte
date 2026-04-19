<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { Harness, SessionsFilter } from '../../types/sessions';

  export let filter: SessionsFilter;
  export let models: string[] = [];
  export let harnesses: Harness[] = [];
  export let count = 0;
  export let total = 0;

  const statuses = ['all', 'completed', 'active', 'cancelled', 'error'];

  const dispatch = createEventDispatcher<{ change: SessionsFilter }>();

  function update(next: Partial<SessionsFilter>): void {
    const merged = { ...filter, ...next };
    dispatch('change', merged);
  }
</script>

<div class="mv-filterbar">
  <div class="mv-search">
    <span class="mv-search-icon" aria-hidden="true">⌕</span>
    <input
      class="mv-search-input"
      placeholder="Search prompts and responses…"
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
      <span class="mv-filter-k">model</span>
      <select class="mv-select" value={filter.model} on:change={(e) => update({ model: e.currentTarget.value })}>
        <option value="all">all</option>
        {#each models as model (model)}
          <option value={model}>{model}</option>
        {/each}
      </select>
    </label>
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
          {#each harnesses as h (h.id)}
            <option value={h.id}>{h.label}</option>
          {/each}
        </select>
      </label>
    {/if}
    <span class="mv-filter-count mono">{count} / {total}</span>
  </div>
</div>

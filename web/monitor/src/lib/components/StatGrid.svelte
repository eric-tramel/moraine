<script context="module" lang="ts">
  let statGridInstanceCounter = 0;

  function slugify(value: string): string {
    return value
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');
  }
</script>

<script lang="ts">
  import type { StatCard } from '../types/ui';

  export let title = '';
  export let cards: StatCard[] = [];

  const instanceId = ++statGridInstanceCounter;

  $: titleSlug = slugify(title);
  $: gridId = `${titleSlug || 'stat-grid'}-${instanceId}`;
</script>

<section>
  <h2>{title}</h2>
  <div class="card-grid" id={gridId}>
    {#if cards.length === 0}
      <article class="card muted">
        <div class="stat-label">Status</div>
        <div class="stat-value">No data loaded</div>
      </article>
    {:else}
      {#each cards as card}
        <article class="card" class:ok={card.tone === 'ok'} class:warn={card.tone === 'warn'} class:bad={card.tone === 'bad'}>
          <div class="stat-label">{card.label}</div>
          <div class="stat-value">{card.value}</div>
        </article>
      {/each}
    {/if}
  </div>
</section>

<script lang="ts">
  import { onDestroy } from 'svelte';
  import SessionCard from '../SessionCard.svelte';
  import SessionDetail from '../SessionDetail.svelte';
  import type { Session } from '../../../types/sessions';

  export let sessions: Session[];

  let openId: string | null = null;

  $: open = sessions.find((s) => s.id === openId) ?? null;

  function handleOpen(s: Session): void {
    openId = s.id;
  }

  function handleClose(): void {
    openId = null;
  }

  function onKey(event: KeyboardEvent): void {
    if (event.key === 'Escape') {
      openId = null;
    }
  }

  $: if (open && typeof window !== 'undefined') {
    window.addEventListener('keydown', onKey);
  } else if (typeof window !== 'undefined') {
    window.removeEventListener('keydown', onKey);
  }

  onDestroy(() => {
    if (typeof window !== 'undefined') {
      window.removeEventListener('keydown', onKey);
    }
  });
</script>

<div class="mv-v1">
  <div class="mv-v1-list mv-list">
    {#if sessions.length === 0}
      <div class="mv-empty">No sessions match these filters.</div>
    {/if}
    {#each sessions as session (session.id)}
      <SessionCard {session} active={session.id === openId} variant="library" on:open={(e) => handleOpen(e.detail)} />
    {/each}
  </div>
  {#if open}
    <div class="mv-sidepanel-backdrop" role="presentation" on:click={handleClose}></div>
    <div class="mv-sidepanel" role="dialog" aria-label="Session detail" aria-modal="true">
      <SessionDetail session={open} layout="sidepanel" closable on:close={handleClose} />
    </div>
  {/if}
</div>

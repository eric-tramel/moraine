<script lang="ts">
  import { onDestroy } from 'svelte';
  import SessionCard from '../SessionCard.svelte';
  import SessionDetail from '../SessionDetail.svelte';
  import { fetchSessionPage } from '../../../api/sessions';
  import type { SessionSummary, SessionTranscript } from '../../../types/sessions';

  /**
   * The session list, and the lazy transcript load behind it.
   *
   * The collapsed list issues no per-session request: it renders the summaries
   * the feed already carries. Opening a session is what triggers the first
   * `/api/v1/sessions/:id/page`, and "load more turns" continues that
   * traversal with the cursor the previous page minted.
   */
  export let sessions: SessionSummary[];

  const TURN_PAGE_SIZE = 25;

  let openId: string | null = null;
  let transcript: SessionTranscript | null = null;
  let turnCursor: string | null = null;
  let turnsHasMore = false;
  let loadingTurns = false;
  let loadingMoreTurns = false;
  let detailError: string | null = null;
  /** Guards against a slow response for a session the user already closed. */
  let requestToken = 0;

  $: open = sessions.find((s) => s.id === openId) ?? null;
  // The open session left the loaded set (a filter change, a refresh): close
  // rather than keep a detail panel with no row behind it.
  $: if (openId !== null && open === null) {
    resetDetail();
  }

  function resetDetail(): void {
    requestToken += 1;
    openId = null;
    transcript = null;
    turnCursor = null;
    turnsHasMore = false;
    loadingTurns = false;
    loadingMoreTurns = false;
    detailError = null;
  }

  function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }

  async function handleOpen(session: SessionSummary): Promise<void> {
    requestToken += 1;
    const token = requestToken;
    openId = session.id;
    transcript = null;
    turnCursor = null;
    turnsHasMore = false;
    detailError = null;
    loadingTurns = true;

    try {
      const page = await fetchSessionPage(session.id, { limit: TURN_PAGE_SIZE });
      if (token !== requestToken) return;
      if (page.reopen) {
        // The pinned view moved underneath us. Page 1 is the documented
        // recovery, and it is this request re-issued from the top.
        const reopened = await fetchSessionPage(session.id, { limit: TURN_PAGE_SIZE });
        if (token !== requestToken) return;
        transcript = reopened.header;
        turnCursor = reopened.nextCursor;
        turnsHasMore = reopened.hasMore;
        return;
      }
      transcript = page.header;
      turnCursor = page.nextCursor;
      turnsHasMore = page.hasMore;
    } catch (error) {
      if (token !== requestToken) return;
      detailError = `Transcript unavailable: ${errorMessage(error)}`;
    } finally {
      if (token === requestToken) loadingTurns = false;
    }
  }

  async function handleLoadMoreTurns(): Promise<void> {
    if (!openId || !turnCursor || loadingMoreTurns) return;
    const token = requestToken;
    const sessionId = openId;
    const cursor = turnCursor;
    loadingMoreTurns = true;

    try {
      const page = await fetchSessionPage(sessionId, { limit: TURN_PAGE_SIZE, cursor });
      if (token !== requestToken) return;
      if (page.reopen) {
        detailError = 'This session changed while loading; reopen it to see the current view.';
        turnsHasMore = false;
        return;
      }
      if (transcript && page.header) {
        transcript = { ...page.header, turns: [...transcript.turns, ...page.turns] };
      } else {
        transcript = page.header;
      }
      turnCursor = page.nextCursor;
      turnsHasMore = page.hasMore;
    } catch (error) {
      if (token !== requestToken) return;
      detailError = `Could not load more turns: ${errorMessage(error)}`;
    } finally {
      if (token === requestToken) loadingMoreTurns = false;
    }
  }

  function onKey(event: KeyboardEvent): void {
    if (event.key === 'Escape') {
      resetDetail();
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
      <SessionCard
        {session}
        active={session.id === openId}
        variant="library"
        on:open={(e) => handleOpen(e.detail)}
      />
    {/each}
  </div>
  {#if open}
    <div class="mv-sidepanel-backdrop" role="presentation" on:click={resetDetail}></div>
    <div class="mv-sidepanel" role="dialog" aria-label="Session detail" aria-modal="true">
      <SessionDetail
        summary={open}
        {transcript}
        loading={loadingTurns}
        loadingMore={loadingMoreTurns}
        hasMore={turnsHasMore}
        errorMessage={detailError}
        layout="sidepanel"
        closable
        on:close={resetDetail}
        on:loadMore={handleLoadMoreTurns}
      />
    </div>
  {/if}
</div>

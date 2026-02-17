<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ThemeMode } from '../types/ui';

  export let theme: ThemeMode = 'light';

  const dispatch = createEventDispatcher<{
    refresh: void;
    toggleTheme: void;
  }>();

  $: themeButtonLabel = theme === 'dark' ? 'Light' : 'Dark';
  $: themeAriaLabel = theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme';
</script>

<header class="top-bar">
  <div>
    <h1>Moraine Monitor</h1>
  </div>
  <div class="top-actions">
    <button
      id="themeToggle"
      type="button"
      class="button-ghost"
      aria-label={themeAriaLabel}
      on:click={() => dispatch('toggleTheme')}
    >
      {themeButtonLabel}
    </button>
    <button id="refreshAll" type="button" class="button-primary" on:click={() => dispatch('refresh')}>
      Refresh All
    </button>
  </div>
</header>

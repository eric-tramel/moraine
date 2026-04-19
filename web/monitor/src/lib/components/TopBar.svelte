<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ThemeMode } from '../types/ui';

  export let theme: ThemeMode = 'light';

  const dispatch = createEventDispatcher<{ setTheme: ThemeMode }>();

  function select(mode: ThemeMode): void {
    if (mode === theme) return;
    dispatch('setTheme', mode);
  }
</script>

<header class="top-bar">
  <div class="brand">
    <h1>Moraine Monitor</h1>
    <span class="brand-live" title="Updating live">live</span>
  </div>
  <div class="top-actions">
    <div class="theme-switch" role="group" aria-label="Theme">
      <button
        id="themeLight"
        type="button"
        aria-pressed={theme === 'light'}
        aria-label="Light theme"
        on:click={() => select('light')}
      >
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="12" cy="12" r="4" />
          <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
        </svg>
      </button>
      <button
        id="themeDark"
        type="button"
        aria-pressed={theme === 'dark'}
        aria-label="Dark theme"
        on:click={() => select('dark')}
      >
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M20 14.5A8 8 0 0 1 9.5 4a7 7 0 1 0 10.5 10.5z" />
        </svg>
      </button>
    </div>
  </div>
</header>

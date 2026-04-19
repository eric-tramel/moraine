<script lang="ts">
  export let tone: 'warn' | 'bad' | null = null;
</script>

<span class="mv-chip" class:mv-chip-warn={tone === 'warn'} class:mv-chip-bad={tone === 'bad'}>
  <slot />
</span>

<script lang="ts">
  import type { Harness } from '../../types/sessions';

  export let harness: Harness;
  export let size = 28;

  $: fontSize = Math.round(size * 0.38);
</script>

<span
  class="mv-avatar"
  style="width: {size}px; height: {size}px; border-radius: {size}px; background: oklch(0.82 0.09 {harness.hue}); color: oklch(0.3 0.1 {harness.hue}); font-size: {fontSize}px;"
>
  {harness.short}
</span>

<script lang="ts">
  import { isSessionActive } from '../../utils/sessionFormat';
  import { nowStore } from '../../state/clock';

  export let endedAt: number;

  $: active = isSessionActive(endedAt, $nowStore);
  $: label = active ? 'active' : 'idle';
</script>

<span class="mv-status" title={label}>
  <span
    class="mv-status-dot"
    class:mv-status-pulse={active}
    class:mv-status-idle={!active}
    style={active ? 'background: #22ff88;' : 'background: var(--subtle);'}
  ></span>
  <span class="mv-status-label">{label}</span>
</span>

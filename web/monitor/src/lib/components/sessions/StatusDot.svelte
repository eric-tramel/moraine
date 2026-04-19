<script lang="ts">
  import { isSessionActive } from '../../utils/sessionFormat';
  import type { SessionStatus } from '../../types/sessions';

  export let status: SessionStatus;
  export let endedAt: number;

  $: active = isSessionActive(status, endedAt);
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

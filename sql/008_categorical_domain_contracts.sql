ALTER TABLE cortex.events
  ADD CONSTRAINT IF NOT EXISTS events_event_kind_domain CHECK event_kind IN (
    'session_meta', 'turn_context', 'message', 'tool_call', 'tool_result', 'reasoning',
    'event_msg', 'compacted_raw', 'progress', 'system', 'summary', 'queue_operation',
    'file_history_snapshot', 'unknown'
  );

ALTER TABLE cortex.events
  ADD CONSTRAINT IF NOT EXISTS events_payload_type_domain CHECK payload_type IN (
    'session_meta', 'turn_context', 'message', 'function_call', 'function_call_output',
    'custom_tool_call', 'custom_tool_call_output', 'web_search_call', 'reasoning',
    'response_item', 'event_msg', 'user_message', 'agent_message', 'agent_reasoning',
    'token_count', 'task_started', 'task_complete', 'turn_aborted', 'item_completed',
    'search_results_received', 'compacted', 'thinking', 'tool_use', 'tool_result', 'text',
    'progress', 'system', 'summary', 'queue-operation', 'file-history-snapshot', 'unknown'
  );

ALTER TABLE cortex.event_links
  ADD CONSTRAINT IF NOT EXISTS event_links_link_type_domain CHECK link_type IN (
    'parent_event', 'compacted_parent', 'parent_uuid', 'tool_use_id', 'source_tool_assistant',
    'unknown'
  );

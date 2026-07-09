ALTER TABLE moraine.event_links
  DROP CONSTRAINT IF EXISTS event_links_link_type_domain;

ALTER TABLE moraine.event_links
  ADD CONSTRAINT event_links_link_type_domain CHECK link_type IN (
    'parent_event', 'compacted_parent', 'parent_uuid', 'tool_use_id', 'source_tool_assistant',
    'subagent_parent', 'unknown'
  );

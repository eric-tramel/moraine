-- Canonicalize historical reasoning metadata after the normalizers moved from
-- the legacy `thinking` label to the canonical `reasoning` label.
--
-- These mutations are idempotent. They cover the canonical event table and
-- the search projections that copy `payload_type` out of `events`.

ALTER TABLE moraine.events
  UPDATE payload_type = 'reasoning',
         content_types = ['reasoning'],
         has_reasoning = 1
  WHERE event_kind = 'reasoning'
    AND (payload_type != 'reasoning'
         OR content_types != ['reasoning']
         OR has_reasoning != 1);

ALTER TABLE moraine.events
  UPDATE content_types = ['reasoning'],
         has_reasoning = 1
  WHERE payload_type = 'agent_reasoning'
    AND (content_types != ['reasoning']
         OR has_reasoning != 1);

ALTER TABLE moraine.search_documents
  UPDATE payload_type = 'reasoning'
  WHERE event_class = 'reasoning'
    AND payload_type != 'reasoning';

ALTER TABLE moraine.search_postings
  UPDATE payload_type = 'reasoning'
  WHERE event_class = 'reasoning'
    AND payload_type != 'reasoning';

ALTER TABLE moraine.search_hit_log
  UPDATE payload_type = 'reasoning'
  WHERE event_class = 'reasoning'
    AND payload_type != 'reasoning';

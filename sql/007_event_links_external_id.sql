ALTER TABLE moraine.event_links
ADD COLUMN IF NOT EXISTS linked_external_id String AFTER linked_event_uid;

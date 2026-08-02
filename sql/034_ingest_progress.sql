ALTER TABLE moraine.ingest_heartbeats
  ADD COLUMN IF NOT EXISTS progress_json String DEFAULT '';

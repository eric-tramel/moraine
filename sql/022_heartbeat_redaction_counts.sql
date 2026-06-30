-- Aggregate secret-redaction counts on ingest heartbeats, encoded as a JSON
-- object `{rule_id: count}`. Redaction itself does not require this column:
-- ingest probes for it at startup and omits the audit surface until migrated.

ALTER TABLE moraine.ingest_heartbeats
  ADD COLUMN IF NOT EXISTS redactions_total String DEFAULT '';

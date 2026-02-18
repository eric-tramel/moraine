ALTER TABLE moraine.search_documents
  ADD COLUMN IF NOT EXISTS has_codex_mcp UInt8
  MATERIALIZED toUInt8(positionCaseInsensitiveUTF8(payload_json, 'codex-mcp') > 0);

ALTER TABLE moraine.search_documents
  MATERIALIZE COLUMN has_codex_mcp;

-- Issue #443: MCP search-result expansion hydrates rows by exact `event_uid`
-- (`WHERE event_uid IN [...]`). `search_documents` is ORDER BY (event_uid),
-- but the sparse primary index can only narrow each lookup to one granule per
-- part — and because event_uids are uniformly distributed hashes, nearly every
-- part's [min, max] key range "could" contain every uid, so each expansion
-- reads a fat-column granule (text_content/payload_json) from every part
-- whether or not the uid is present there. During the incident this amplified
-- to hundreds of MiB read per expansion call. A per-granule bloom filter lets
-- ClickHouse skip the granules that do not actually contain the uid, which is
-- most of them: a uid lives in exactly one part once merges settle.
ALTER TABLE moraine.search_documents
  ADD INDEX IF NOT EXISTS idx_search_documents_event_uid_bloom event_uid TYPE bloom_filter(0.01) GRANULARITY 1;

-- Build the index for existing parts in the background; new parts index at
-- insert/merge time. Deliberately not `mutations_sync = 1`: materializing over
-- a large table must not block startup migrations, and queries are merely
-- unaccelerated (not wrong) until the mutation finishes.
ALTER TABLE moraine.search_documents
  MATERIALIZE INDEX idx_search_documents_event_uid_bloom;

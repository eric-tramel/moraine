CREATE TABLE IF NOT EXISTS moraine.search_conversation_terms (
  term String,
  session_id String,
  tf_sum UInt64,
  event_freq UInt32
)
ENGINE = SummingMergeTree
PARTITION BY cityHash64(term) % 32
ORDER BY (term, session_id);

TRUNCATE TABLE moraine.search_conversation_terms;

INSERT INTO moraine.search_conversation_terms
SELECT
  term,
  session_id,
  toUInt64(sum(toUInt64(tf))) AS tf_sum,
  toUInt32(count()) AS event_freq
FROM moraine.search_postings FINAL
GROUP BY term, session_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_search_conversation_terms
TO moraine.search_conversation_terms
AS
SELECT
  term,
  session_id,
  toUInt64(tf) AS tf_sum,
  toUInt32(1) AS event_freq
FROM moraine.search_postings;

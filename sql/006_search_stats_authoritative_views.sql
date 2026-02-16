DROP VIEW IF EXISTS cortex.mv_search_term_stats;
DROP VIEW IF EXISTS cortex.mv_search_corpus_stats;

DROP TABLE IF EXISTS cortex.search_term_stats;
DROP TABLE IF EXISTS cortex.search_corpus_stats;

CREATE VIEW IF NOT EXISTS cortex.search_term_stats
AS
SELECT
  term,
  toUInt64(count()) AS docs
FROM cortex.search_postings FINAL
GROUP BY term;

CREATE VIEW IF NOT EXISTS cortex.search_corpus_stats
AS
SELECT
  toUInt8(0) AS bucket,
  toUInt64(count()) AS docs,
  toUInt64(ifNull(sum(doc_len), 0)) AS total_doc_len
FROM cortex.search_documents FINAL
WHERE doc_len > 0;

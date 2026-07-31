WITH
  heads AS (
    SELECT source_host, source_name, source_file, source_generation, published_at
    FROM `moraine`.v_current_published_source_generations
  ),
  retired AS (
    SELECT h.source_host AS source_host, h.source_name AS source_name,
           h.source_file AS source_file, h.source_generation AS source_generation
    FROM `moraine`.v_published_source_generation_history AS h
    INNER JOIN heads AS head
      ON head.source_host = h.source_host
     AND head.source_name = h.source_name
     AND head.source_file = h.source_file
    WHERE h.source_generation != head.source_generation
      AND head.published_at < now64(3) - toIntervalSecond(604800)
  ),
  cg_rollup AS (
    SELECT source_host, source_name, source_file, source_generation,
         toUInt64(count()) AS event_rows, toUInt64(0) AS raw_rows,
         toUInt64(0) AS error_rows, toUInt64(0) AS document_rows
    FROM `moraine`.events
    WHERE (source_host, source_name, source_file, source_generation) IN (
      SELECT source_host, source_name, source_file, source_generation FROM retired
    )
    GROUP BY source_host, source_name, source_file, source_generation
    UNION ALL
    SELECT source_host, source_name, source_file, source_generation,
         toUInt64(0) AS event_rows, toUInt64(count()) AS raw_rows,
         toUInt64(0) AS error_rows, toUInt64(0) AS document_rows
    FROM `moraine`.raw_events
    WHERE (source_host, source_name, source_file, source_generation) IN (
      SELECT source_host, source_name, source_file, source_generation FROM retired
    )
    GROUP BY source_host, source_name, source_file, source_generation
    UNION ALL
    SELECT source_host, source_name, source_file, source_generation,
         toUInt64(0) AS event_rows, toUInt64(0) AS raw_rows,
         toUInt64(count()) AS error_rows, toUInt64(0) AS document_rows
    FROM `moraine`.ingest_errors
    WHERE (source_host, source_name, source_file, source_generation) IN (
      SELECT source_host, source_name, source_file, source_generation FROM retired
    )
    GROUP BY source_host, source_name, source_file, source_generation
    UNION ALL
    SELECT source_host, source_name, source_file, source_generation,
         toUInt64(0) AS event_rows, toUInt64(0) AS raw_rows,
         toUInt64(0) AS error_rows, toUInt64(count()) AS document_rows
    FROM `moraine`.search_documents
    WHERE (source_host, source_name, source_file, source_generation) IN (
      SELECT source_host, source_name, source_file, source_generation FROM retired
    )
    GROUP BY source_host, source_name, source_file, source_generation
  )
SELECT r.source_host AS source_host, r.source_name AS source_name,
       r.source_file AS source_file, toUInt32(r.source_generation) AS source_generation,
       toUInt64(sum(cg_rollup.event_rows)) AS event_rows,
       toUInt64(sum(cg_rollup.raw_rows)) AS raw_rows,
       toUInt64(sum(cg_rollup.error_rows)) AS error_rows,
       toUInt64(sum(cg_rollup.document_rows)) AS document_rows
FROM retired AS r
INNER JOIN cg_rollup ON cg_rollup.source_host = r.source_host
  AND cg_rollup.source_name = r.source_name
  AND cg_rollup.source_file = r.source_file
  AND cg_rollup.source_generation = r.source_generation
GROUP BY r.source_host, r.source_name, r.source_file, r.source_generation
ORDER BY source_host ASC, source_name ASC, source_file ASC, source_generation ASC
LIMIT 512
FORMAT TSVWithNames

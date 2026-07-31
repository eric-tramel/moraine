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
      AND head.published_at < now64(3) - toIntervalSecond(86400)
  ),
  ri_rollup AS (
    SELECT source_host, source_name, source_file, source_generation,
         toUInt64(count()) AS navigation_rows, toUInt64(0) AS locator_rows,
         toUInt64(0) AS directory_rows
    FROM `moraine`.mcp_event_navigation
    WHERE (source_host, source_name, source_file, source_generation) IN (
      SELECT source_host, source_name, source_file, source_generation FROM retired
    )
    GROUP BY source_host, source_name, source_file, source_generation
    UNION ALL
    SELECT source_host, source_name, source_file, source_generation,
         toUInt64(0) AS navigation_rows, toUInt64(count()) AS locator_rows,
         toUInt64(0) AS directory_rows
    FROM `moraine`.mcp_event_locator
    WHERE (source_host, source_name, source_file, source_generation) IN (
      SELECT source_host, source_name, source_file, source_generation FROM retired
    )
    GROUP BY source_host, source_name, source_file, source_generation
    UNION ALL
    SELECT source_host, source_name, source_file, source_generation,
         toUInt64(0) AS navigation_rows, toUInt64(0) AS locator_rows,
         toUInt64(count()) AS directory_rows
    FROM `moraine`.mcp_session_directory
    WHERE (source_host, source_name, source_file, source_generation) IN (
      SELECT source_host, source_name, source_file, source_generation FROM retired
    )
    GROUP BY source_host, source_name, source_file, source_generation
  )
SELECT r.source_host AS source_host, r.source_name AS source_name,
       r.source_file AS source_file, toUInt32(r.source_generation) AS source_generation,
       toUInt64(sum(ri_rollup.navigation_rows)) AS navigation_rows,
       toUInt64(sum(ri_rollup.locator_rows)) AS locator_rows,
       toUInt64(sum(ri_rollup.directory_rows)) AS directory_rows
FROM retired AS r
INNER JOIN ri_rollup ON ri_rollup.source_host = r.source_host
  AND ri_rollup.source_name = r.source_name
  AND ri_rollup.source_file = r.source_file
  AND ri_rollup.source_generation = r.source_generation
GROUP BY r.source_host, r.source_name, r.source_file, r.source_generation
ORDER BY source_host ASC, source_name ASC, source_file ASC, source_generation ASC
LIMIT 512
FORMAT TSVWithNames

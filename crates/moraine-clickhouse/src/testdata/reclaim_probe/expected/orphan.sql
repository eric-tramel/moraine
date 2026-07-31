SELECT child.session_id AS session_id,
       toUInt64(child.candidate_generation) AS candidate_generation,
       toUInt64(sum(child.event_rows)) AS event_rows,
       toUInt64(sum(child.turn_rows)) AS turn_rows,
       toUInt64(0) AS header_rows
FROM (
    SELECT session_id, candidate_generation, toUInt64(count()) AS event_rows,
         toUInt64(0) AS turn_rows, max(projected_at) AS newest
    FROM `moraine`.mcp_open_events
    GROUP BY session_id, candidate_generation
    UNION ALL
    SELECT session_id, candidate_generation, toUInt64(0) AS event_rows,
         toUInt64(count()) AS turn_rows, max(projected_at) AS newest
    FROM `moraine`.mcp_open_turns
    GROUP BY session_id, candidate_generation
  ) AS child
WHERE (child.session_id, child.candidate_generation) NOT IN (
    SELECT session_id, generation FROM `moraine`.mcp_open_publication_headers
  )
  AND (child.session_id, child.candidate_generation) NOT IN (
    SELECT session_id, candidate_generation
    FROM `moraine`.mcp_open_backfill_plans FINAL
    WHERE phase != 4
  )
GROUP BY child.session_id, child.candidate_generation
HAVING max(child.newest) < now64(3) - toIntervalSecond(86400)
ORDER BY session_id ASC, candidate_generation ASC
LIMIT 512
FORMAT TSVWithNames

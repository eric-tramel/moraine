WITH
  headers AS (
    SELECT session_id, generation, header_revision, tombstone,
           required_heads_fingerprint, dirty_revision, prepared_at, required_source_heads
    FROM `moraine`.mcp_open_publication_headers FINAL
  ),
  required AS (
    SELECT session_id, generation, head.1 AS source_host, head.2 AS source_name,
           head.3 AS source_file, head.4 AS source_generation
    FROM headers ARRAY JOIN required_source_heads AS head
  ),
  unpublished AS (
    SELECT DISTINCT session_id, generation
    FROM required
    WHERE (source_host, source_name, source_file, source_generation) NOT IN (
      SELECT source_host, source_name, source_file, source_generation
      FROM `moraine`.v_current_published_source_generations
    )
  ),
  live AS (
    SELECT h.session_id AS session_id, h.header_revision AS live_header_revision,
           h.generation AS live_generation,
           h.required_heads_fingerprint AS live_fingerprint, h.prepared_at AS live_prepared_at,
           d.dirty_revision AS current_dirty
    FROM headers AS h
    INNER JOIN (
      SELECT session_id, generation FROM `moraine`.mcp_open_sessions FINAL
    ) AS p ON p.session_id = h.session_id AND p.generation = h.generation
    INNER JOIN (
      SELECT session_id, dirty_revision FROM `moraine`.mcp_open_dirty_sessions FINAL
    ) AS d ON d.session_id = h.session_id
    WHERE h.tombstone = 0
      AND length(h.required_source_heads) > 0
      AND h.dirty_revision = d.dirty_revision
      AND (h.session_id, h.generation) NOT IN (SELECT session_id, generation FROM unpublished)
      AND h.prepared_at < now64(3) - toIntervalSecond(86400)
  ),
  retired AS (
    SELECT h.session_id AS session_id, h.generation AS generation
    FROM headers AS h
    INNER JOIN live AS l ON l.session_id = h.session_id
    WHERE h.generation != l.live_generation
    GROUP BY h.session_id, h.generation
    HAVING countIf(h.header_revision < l.live_header_revision
             AND h.dirty_revision < l.current_dirty
             AND h.required_heads_fingerprint != l.live_fingerprint) = count()
  ),
  child AS (
    SELECT session_id, candidate_generation, toUInt64(count()) AS event_rows,
         toUInt64(0) AS turn_rows, max(projected_at) AS newest
    FROM `moraine`.mcp_open_events
    GROUP BY session_id, candidate_generation
    UNION ALL
    SELECT session_id, candidate_generation, toUInt64(0) AS event_rows,
         toUInt64(count()) AS turn_rows, max(projected_at) AS newest
    FROM `moraine`.mcp_open_turns
    GROUP BY session_id, candidate_generation
  )
SELECT r.session_id AS session_id,
       toUInt64(r.generation) AS candidate_generation,
       toUInt64(sum(ifNull(child.event_rows, 0))) AS event_rows,
       toUInt64(sum(ifNull(child.turn_rows, 0))) AS turn_rows,
       toUInt64(1) AS header_rows
FROM retired AS r
LEFT JOIN child ON child.session_id = r.session_id
  AND child.candidate_generation = r.generation
GROUP BY r.session_id, r.generation
HAVING max(ifNull(child.newest, toDateTime64(0, 3))) < now64(3) - toIntervalSecond(86400)
ORDER BY session_id ASC, candidate_generation ASC
LIMIT 512
FORMAT TSVWithNames

-- Rebuild existing OMP session heads so source titles and privacy-safe
-- dispatched-session labels are reflected by the MCP open read model.
INSERT INTO moraine.mcp_open_dirty_sessions
  (session_id, dirty_revision, observed_at)
SELECT session_id, generateSnowflakeID(), now64(3)
FROM (
  SELECT DISTINCT session_id
  FROM moraine.events FINAL
  WHERE notEmpty(session_id)
    AND source_name = 'omp'
);

-- SUPERSEDED 2026-08-01 (issue #603 WI-10, `sql/041`). The `mcp_open_*`
-- projection this file names is dropped by migration 041; the code that ships
-- 041 carries no projector, no v1 reader and no `mcp_open` reclaim executor,
-- so every present-tense description of that machinery here records the state
-- when this file was written, not a running component. Nothing else in this
-- file is edited and every statement in it still executes verbatim — a fresh
-- install creates the family and 041 drops it in the same migrate pass. A
-- released migration is immutable and the runner keys applied migrations by
-- `(version, name)` with no content checksum, so an upgraded host never
-- re-reads this file: this note is append-only, and it is here for the
-- operator who reads the file on a fresh install.
--
-- It sits at the FOOT of this file on purpose. Source elsewhere in the tree
-- cites these statements by `sql/NNN:LINE`, and a note at the head moves
-- every one of those citations silently; appended here, no existing line
-- number changes. `a_cross_file_line_citation_resolves_to_what_it_claims`
-- is what keeps them honest either way.

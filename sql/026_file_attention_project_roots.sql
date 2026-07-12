-- Durable canonical project mapping for worktree roots.
--
-- The mapping lets project-scoped file_attention attribute pre-digest rows
-- after a linked worktree is deleted or pruned. New normalized event/tool rows
-- populate it automatically; existing digest rows are backfilled once.

CREATE TABLE IF NOT EXISTS moraine.file_attention_project_roots (
  project_id LowCardinality(String),
  worktree_root String,
  observed_version UInt64
)
ENGINE = ReplacingMergeTree(observed_version)
ORDER BY (project_id, worktree_root);

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_file_attention_project_roots_from_events
TO moraine.file_attention_project_roots
AS
SELECT project_id, worktree_root, event_version AS observed_version
FROM moraine.events
WHERE startsWith(project_id, 'git:') AND worktree_root != '';

CREATE MATERIALIZED VIEW IF NOT EXISTS moraine.mv_file_attention_project_roots_from_tool_io
TO moraine.file_attention_project_roots
AS
SELECT project_id, worktree_root, event_version AS observed_version
FROM moraine.tool_io
WHERE startsWith(project_id, 'git:') AND worktree_root != '';

INSERT INTO moraine.file_attention_project_roots (project_id, worktree_root, observed_version)
SELECT project_id, worktree_root, max(event_version)
FROM (
  SELECT project_id, worktree_root, event_version
  FROM moraine.events FINAL
  WHERE startsWith(project_id, 'git:') AND worktree_root != ''
  UNION ALL
  SELECT project_id, worktree_root, event_version
  FROM moraine.tool_io FINAL
  WHERE startsWith(project_id, 'git:') AND worktree_root != ''
)
GROUP BY project_id, worktree_root;

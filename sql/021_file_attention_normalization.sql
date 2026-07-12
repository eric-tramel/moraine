-- Forward-only normalization columns for file_attention Phase 1.
--
-- Existing rows read these as defaults. New ingest rows fill them only when a
-- repo/worktree root, Git common-directory identity, and project backend marker
-- can be proven; Tier 0 suffix matching remains the permanent fallback for
-- legacy and unnormalizable rows.

ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS project_id LowCardinality(String) DEFAULT '';

ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS repo_rel_path String DEFAULT '';

ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS worktree_root String DEFAULT '';

ALTER TABLE moraine.tool_io
  ADD COLUMN IF NOT EXISTS project_id LowCardinality(String) DEFAULT '';

ALTER TABLE moraine.tool_io
  ADD COLUMN IF NOT EXISTS repo_rel_path String DEFAULT '';

ALTER TABLE moraine.tool_io
  ADD COLUMN IF NOT EXISTS worktree_root String DEFAULT '';

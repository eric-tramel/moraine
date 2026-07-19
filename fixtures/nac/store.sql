-- Sanitized fixture for arcee-ai/nac@a23a3041b383e68c55cc358709096c97c58cfcd4.
-- Schema source: crates/nac-core/src/store/schema.rs at that revision.
-- Regenerate after creating a sanitized database with the pinned schema:
--   sqlite3 sanitized-nac-store.db '.dump' > fixtures/nac/store.sql

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE sessions (
    session_id TEXT PRIMARY KEY,
    cwd TEXT NOT NULL,
    model TEXT NOT NULL,
    base_url TEXT NOT NULL,
    backend TEXT,
    reasoning_effort TEXT,
    sandbox_json TEXT,
    messages_json TEXT NOT NULL,
    api_key_env TEXT,
    extra_headers_json TEXT,
    last_response_duration_ms INTEGER,
    previous_response_duration_ms INTEGER,
    response_durations_json TEXT,
    token_usages_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    host_id TEXT
);

CREATE TABLE episodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    action TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
);

INSERT INTO sessions (
    session_id, cwd, model, base_url, backend, reasoning_effort,
    sandbox_json, messages_json, api_key_env, extra_headers_json,
    last_response_duration_ms, previous_response_duration_ms,
    response_durations_json, token_usages_json, created_at, updated_at, host_id
) VALUES (
    'session-local', '/workspace/local', 'z-ai/glm-5.2', 'https://openrouter.ai/api/v1',
    'together-chat', 'low', '{"backend":"local"}',
    '[{"role":"system","content":"system prompt"},{"role":"user","content":"hello"},{"role":"assistant","content":null,"reasoning_text":"inspect first","reasoning_details":{"kind":"analysis"},"tool_calls":[{"id":"call-1","type":"function","function":{"name":"mcp__moraine__search","arguments":"{\"query\":\"nac\"}"}}]},{"role":"tool","tool_call_id":"call-1","content":"{\"result\":\"ok\"}"},{"role":"assistant","content":"done"}]',
    'NAC_FIXTURE_SECRET_ENV', '{"Authorization":"fixture-secret"}', 1200, 900,
    '[1200]', '[{"input_tokens":12,"output_tokens":4,"cache_read_tokens":2}]',
    '2026-07-18 12:15:51.775682000', '2026-07-18 12:16:58.123456000', NULL
);

INSERT INTO sessions (
    session_id, cwd, model, base_url, messages_json, created_at, updated_at, host_id
) VALUES (
    'session-remote', '/remote/private/path', 'remote-model', 'https://api.openai.com/v1',
    '[{"role":"user","content":"remote question"},{"role":"assistant","content":"remote answer"}]',
    '2026-07-18 13:00:00.000000000', '2026-07-18 13:00:01.000000000', 'remote-host'
);

INSERT INTO episodes (thread_name, session_id, action, content, created_at) VALUES
    ('worker-a', 'session-local', 'inspect repository', 'inspection complete', '2026-07-18 12:16:01'),
    ('worker-b', 'session-local', 'run checks', 'checks complete', '2026-07-18 12:16:02'),
    ('worker-remote', 'session-remote', 'remote private action', 'remote private response', '2026-07-18 13:00:00');

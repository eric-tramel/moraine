CREATE TABLE moraine_schema_migrations (
    version UINTEGER PRIMARY KEY,
    filename VARCHAR NOT NULL,
    checksum VARCHAR NOT NULL,
    applied_at TIMESTAMP NOT NULL
);

CREATE TABLE publication_heads (
    source_name VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_generation UBIGINT NOT NULL,
    publication_revision UBIGINT NOT NULL,
    cache_epoch UBIGINT NOT NULL,
    published_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (source_name, source_file)
);

CREATE TABLE events (
    event_uid VARCHAR NOT NULL,
    event_version UBIGINT NOT NULL,
    source_name VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_generation UBIGINT NOT NULL,
    session_id VARCHAR NOT NULL,
    turn_id VARCHAR,
    event_type VARCHAR NOT NULL,
    event_time VARCHAR,
    event_order UBIGINT NOT NULL,
    payload_json VARCHAR NOT NULL,
    PRIMARY KEY (source_name, source_file, source_generation, event_uid, event_version)
);

CREATE TABLE event_staging (
    operation_id VARCHAR NOT NULL,
    event_uid VARCHAR NOT NULL,
    event_version UBIGINT NOT NULL,
    source_name VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_generation UBIGINT NOT NULL,
    session_id VARCHAR NOT NULL,
    turn_id VARCHAR,
    event_type VARCHAR NOT NULL,
    event_time VARCHAR,
    event_order UBIGINT NOT NULL,
    payload_json VARCHAR NOT NULL
);

CREATE TABLE raw_events (
    operation_id VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_generation UBIGINT NOT NULL,
    row_json VARCHAR NOT NULL
);

CREATE TABLE event_links (
    operation_id VARCHAR NOT NULL,
    row_json VARCHAR NOT NULL
);

CREATE TABLE ingest_errors (
    operation_id VARCHAR NOT NULL,
    row_json VARCHAR NOT NULL
);

CREATE TABLE ingest_checkpoints (
    source_name VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_generation UBIGINT NOT NULL,
    source_offset UBIGINT NOT NULL,
    checkpoint_revision UBIGINT NOT NULL,
    operation_id VARCHAR NOT NULL,
    checkpoint_json VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (source_name, source_file)
);

CREATE TABLE ingest_heartbeats (
    observed_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    row_json VARCHAR NOT NULL
);

CREATE TABLE ingest_progress (
    observed_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    row_json VARCHAR NOT NULL
);

CREATE TABLE search_postings (
    source_name VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_generation UBIGINT NOT NULL,
    event_uid VARCHAR NOT NULL,
    event_version UBIGINT NOT NULL,
    term VARCHAR NOT NULL,
    term_frequency UINTEGER NOT NULL,
    document_length UINTEGER NOT NULL,
    PRIMARY KEY (source_name, source_file, source_generation, event_uid, event_version, term)
);

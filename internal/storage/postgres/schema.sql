CREATE TABLE IF NOT EXISTS conversions (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT        NOT NULL UNIQUE,
    source_url      TEXT        NOT NULL,
    target_format   TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    result_url      TEXT        NOT NULL DEFAULT '',
    error           TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- Частковий індекс: relay шукає лише невідправлені повідомлення
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
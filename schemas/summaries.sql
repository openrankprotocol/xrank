CREATE TABLE xrank.community_summaries (
    id BIGSERIAL PRIMARY KEY,
    community_id BIGINT NOT NULL UNIQUE,
    summary JSONB NOT NULL,
    topic TEXT,
    few_words TEXT,
    one_sentence TEXT,
    error TEXT,
    model TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX idx_community_summaries_created_at
    ON xrank.community_summaries (created_at DESC);

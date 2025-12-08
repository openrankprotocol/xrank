CREATE TABLE xrank.community_summaries (
    id BIGSERIAL PRIMARY KEY,
    community_id BIGINT NOT NULL,
    run_id INTEGER,
    posts_limit INTEGER NOT NULL,
    summary JSONB NOT NULL,
    topic TEXT,
    few_words TEXT,
    one_sentence TEXT,
    error TEXT,
    model TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE UNIQUE INDEX community_summaries_community_null_run_unique
    ON xrank.community_summaries (community_id)
    WHERE run_id IS NULL;


CREATE UNIQUE INDEX community_summaries_community_run_unique
    ON xrank.community_summaries (community_id, run_id)
    WHERE run_id IS NOT NULL;


CREATE INDEX idx_community_summaries_created_at
    ON xrank.community_summaries (created_at DESC);

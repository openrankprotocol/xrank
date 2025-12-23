-- Seed Graph Scores table
-- Stores computed user scores for each seed graph run

CREATE TABLE IF NOT EXISTS xrank_seed.scores (
    community_id TEXT NOT NULL,             -- Community identifier (e.g., "base_latam")
    run_id INTEGER NOT NULL,                -- Run ID (per community)
    user_id BIGINT NOT NULL,                -- User ID (reference to users.user_id)
    score DOUBLE PRECISION NOT NULL,        -- Computed score value
    PRIMARY KEY (community_id, run_id, user_id),
    FOREIGN KEY (community_id, run_id) REFERENCES xrank_seed.runs(community_id, run_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES xrank.users(user_id) ON DELETE CASCADE
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_seed_scores_community_run ON xrank_seed.scores(community_id, run_id);
CREATE INDEX IF NOT EXISTS idx_seed_scores_user ON xrank_seed.scores(user_id);
CREATE INDEX IF NOT EXISTS idx_seed_scores_score ON xrank_seed.scores(score DESC);
CREATE INDEX IF NOT EXISTS idx_seed_scores_community_run_score ON xrank_seed.scores(community_id, run_id, score DESC);

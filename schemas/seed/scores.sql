-- Seed Graph Scores table
-- Stores computed user scores for each seed graph run

CREATE TABLE IF NOT EXISTS xrank_seed.scores (
    seed_graph_id TEXT NOT NULL,            -- Seed graph identifier (e.g., "base_latam")
    run_id INTEGER NOT NULL,                -- Run ID (per seed graph)
    user_id BIGINT NOT NULL,                -- User ID (reference to users.user_id)
    score DOUBLE PRECISION NOT NULL,        -- Computed score value
    PRIMARY KEY (seed_graph_id, run_id, user_id),
    FOREIGN KEY (seed_graph_id, run_id) REFERENCES xrank_seed.runs(seed_graph_id, run_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES xrank.users(user_id) ON DELETE CASCADE
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_seed_scores_seed_graph_run ON xrank_seed.scores(seed_graph_id, run_id);
CREATE INDEX IF NOT EXISTS idx_seed_scores_user ON xrank_seed.scores(user_id);
CREATE INDEX IF NOT EXISTS idx_seed_scores_score ON xrank_seed.scores(score DESC);
CREATE INDEX IF NOT EXISTS idx_seed_scores_seed_graph_run_score ON xrank_seed.scores(seed_graph_id, run_id, score DESC);

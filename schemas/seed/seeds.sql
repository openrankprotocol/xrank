-- Seed Graph Seeds table
-- Stores seed users for each seed graph computation run

CREATE TABLE IF NOT EXISTS xrank_seed.seeds (
    seed_graph_id TEXT NOT NULL,            -- Seed graph identifier (e.g., "base_latam")
    run_id INTEGER NOT NULL,                -- Run ID (per seed graph)
    user_id BIGINT NOT NULL,                -- User ID (reference to users.user_id)
    score DOUBLE PRECISION NOT NULL,        -- Seed score value
    PRIMARY KEY (seed_graph_id, run_id, user_id),
    FOREIGN KEY (seed_graph_id, run_id) REFERENCES xrank_seed.runs(seed_graph_id, run_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES xrank.users(user_id) ON DELETE CASCADE
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_seed_seeds_seed_graph_run ON xrank_seed.seeds(seed_graph_id, run_id);
CREATE INDEX IF NOT EXISTS idx_seed_seeds_user ON xrank_seed.seeds(user_id);
CREATE INDEX IF NOT EXISTS idx_seed_seeds_seed_graph_run_score ON xrank_seed.seeds(seed_graph_id, run_id, score DESC);

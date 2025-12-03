-- Seeds table
-- Stores seed users for each computation run

CREATE TABLE IF NOT EXISTS xrank.seeds (
    community_id BIGINT NOT NULL,           -- Community ID
    run_id INTEGER NOT NULL,                -- Run ID (per community)
    user_id BIGINT NOT NULL,                -- User ID (reference to users.user_id)
    score DOUBLE PRECISION NOT NULL,        -- Seed score value
    PRIMARY KEY (community_id, run_id, user_id),
    FOREIGN KEY (community_id, run_id) REFERENCES xrank.runs(community_id, run_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES xrank.users(user_id) ON DELETE CASCADE
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_seeds_community_run ON xrank.seeds(community_id, run_id);
CREATE INDEX IF NOT EXISTS idx_seeds_user ON xrank.seeds(user_id);
CREATE INDEX IF NOT EXISTS idx_seeds_community_run_score ON xrank.seeds(community_id, run_id, score DESC);

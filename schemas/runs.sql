-- Runs table
-- Tracks each score computation run with versioning per community

CREATE TABLE IF NOT EXISTS xrank.runs (
    community_id BIGINT NOT NULL,           -- Community this run is for
    run_id INTEGER NOT NULL,                -- Run ID (incrementing per community)
    days_back INTEGER,                      -- Number of days of data used
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When this run was created
    PRIMARY KEY (community_id, run_id),
    FOREIGN KEY (community_id) REFERENCES xrank.communities(community_id) ON DELETE CASCADE
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_runs_community ON xrank.runs(community_id);
CREATE INDEX IF NOT EXISTS idx_runs_created_at ON xrank.runs(created_at);
CREATE INDEX IF NOT EXISTS idx_runs_community_created ON xrank.runs(community_id, created_at DESC);

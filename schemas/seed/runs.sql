-- Seed Graph Runs table
-- Tracks each score computation run for seed graphs (no community association)

CREATE SCHEMA IF NOT EXISTS xrank_seed;

CREATE TABLE IF NOT EXISTS xrank_seed.runs (
    seed_graph_id TEXT NOT NULL,            -- Seed graph identifier (e.g., "base_latam")
    run_id INTEGER NOT NULL,                -- Run ID (incrementing per seed graph)
    days_back INTEGER,                      -- Number of days of data used
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When this run was created
    PRIMARY KEY (seed_graph_id, run_id)
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_seed_runs_seed_graph ON xrank_seed.runs(seed_graph_id);
CREATE INDEX IF NOT EXISTS idx_seed_runs_created_at ON xrank_seed.runs(created_at);
CREATE INDEX IF NOT EXISTS idx_seed_runs_seed_graph_created ON xrank_seed.runs(seed_graph_id, created_at DESC);

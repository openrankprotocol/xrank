-- Communities table
-- Stores X/Twitter community information

CREATE TABLE IF NOT EXISTS xrank.communities (
    community_id BIGINT PRIMARY KEY,        -- X community ID (e.g., 1896991026272723220)
    name TEXT,                              -- Community name (e.g., "Story")
    description TEXT,                       -- Community description
    created_at TIMESTAMP,                   -- When the community was created on X
    creator_id BIGINT,                      -- User ID of the community creator
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When we imported this community
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP    -- Last update timestamp
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_communities_name ON xrank.communities(name);
CREATE INDEX IF NOT EXISTS idx_communities_created_at ON xrank.communities(created_at);

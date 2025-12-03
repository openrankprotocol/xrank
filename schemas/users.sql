-- Users/Members table
-- Stores X/Twitter user information and their membership in communities

CREATE TABLE IF NOT EXISTS xrank.users (
    user_id BIGINT PRIMARY KEY,             -- X user ID (e.g., 1395085833825292289)
    username TEXT NOT NULL,                 -- X username/handle (e.g., "ElZamani19")
    display_name TEXT,                      -- Display name (e.g., "El zamani")
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When we imported this user
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP    -- Last update timestamp
);

-- Community membership junction table
-- Links users to communities with their role
CREATE TABLE IF NOT EXISTS xrank.community_members (
    community_id BIGINT NOT NULL,           -- Reference to communities.community_id
    user_id BIGINT NOT NULL,                -- Reference to users.user_id
    role TEXT NOT NULL DEFAULT 'member',    -- Role: 'member', 'moderator', 'admin'
    removed BOOLEAN DEFAULT FALSE,          -- Whether the membership has been removed
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When we imported this membership
    PRIMARY KEY (community_id, user_id),
    FOREIGN KEY (community_id) REFERENCES communities(community_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_username_lower ON users(LOWER(username));
CREATE INDEX IF NOT EXISTS idx_users_verified ON users(verified);

CREATE INDEX IF NOT EXISTS idx_community_members_community ON community_members(community_id);
CREATE INDEX IF NOT EXISTS idx_community_members_user ON community_members(user_id);
CREATE INDEX IF NOT EXISTS idx_community_members_role ON community_members(role);

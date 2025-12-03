-- Followings table
-- Stores follow relationships between users within a community context

CREATE TABLE IF NOT EXISTS xrank.followings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,   -- Auto-incrementing primary key
    follower_user_id BIGINT NOT NULL,       -- User who is following (reference to users.user_id)
    following_user_id BIGINT NOT NULL,      -- User being followed (reference to users.user_id)
    community_id BIGINT,                    -- Community context where this relationship was observed
    removed BOOLEAN DEFAULT FALSE,          -- Whether the follow relationship has been removed
    UNIQUE (follower_user_id, following_user_id, community_id),
    FOREIGN KEY (follower_user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (following_user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (community_id) REFERENCES communities(community_id) ON DELETE SET NULL
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_followings_follower ON followings(follower_user_id);
CREATE INDEX IF NOT EXISTS idx_followings_following ON followings(following_user_id);
CREATE INDEX IF NOT EXISTS idx_followings_community ON followings(community_id);

-- Composite index for querying mutual follows
CREATE INDEX IF NOT EXISTS idx_followings_pair ON followings(follower_user_id, following_user_id);

-- Index for finding all follows within a community
CREATE INDEX IF NOT EXISTS idx_followings_community_follower ON followings(community_id, follower_user_id);

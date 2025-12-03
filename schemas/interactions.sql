-- Interactions table
-- Stores all types of interactions: posts, replies, comments, quotes, retweets

CREATE TABLE IF NOT EXISTS xrank.interactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,   -- Auto-incrementing primary key
    post_id BIGINT NOT NULL,                -- X post/tweet ID (e.g., 1988863621917507822)
    interaction_type TEXT NOT NULL,         -- Type: 'post', 'reply', 'comment', 'quote', 'retweet'
    community_id BIGINT,                    -- Community where this was observed (NULL if global post)

    -- Author information
    author_user_id BIGINT NOT NULL,         -- User who created this interaction (reference to users.user_id)

    -- Content
    text TEXT,                              -- Text content of the interaction
    created_at TIMESTAMP,                   -- When the interaction was created on X

    -- Reply-specific fields
    reply_to_post_id BIGINT,                -- Post ID being replied to (for replies/comments)
    reply_to_user_id BIGINT,                -- User ID being replied to

    -- Retweet-specific fields
    retweeted_post_id BIGINT,               -- Original post ID (for retweets)
    retweeted_user_id BIGINT,               -- Original post author ID

    -- Quote-specific fields
    quoted_post_id BIGINT,                  -- Quoted post ID (for quote tweets)
    quoted_user_id BIGINT,                  -- Quoted post author ID

    -- Metadata
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When we imported this interaction

    UNIQUE (post_id, community_id),
    FOREIGN KEY (author_user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (community_id) REFERENCES communities(community_id) ON DELETE SET NULL,
    FOREIGN KEY (reply_to_user_id) REFERENCES users(user_id) ON DELETE SET NULL,
    FOREIGN KEY (retweeted_user_id) REFERENCES users(user_id) ON DELETE SET NULL,
    FOREIGN KEY (quoted_user_id) REFERENCES users(user_id) ON DELETE SET NULL
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_interactions_post_id ON interactions(post_id);
CREATE INDEX IF NOT EXISTS idx_interactions_type ON interactions(interaction_type);
CREATE INDEX IF NOT EXISTS idx_interactions_community ON interactions(community_id);
CREATE INDEX IF NOT EXISTS idx_interactions_author ON interactions(author_user_id);
CREATE INDEX IF NOT EXISTS idx_interactions_created_at ON interactions(created_at);

-- Indexes for finding replies/comments to specific posts or users
CREATE INDEX IF NOT EXISTS idx_interactions_reply_to_post ON interactions(reply_to_post_id);
CREATE INDEX IF NOT EXISTS idx_interactions_reply_to_user ON interactions(reply_to_user_id);

-- Indexes for finding retweets and quotes
CREATE INDEX IF NOT EXISTS idx_interactions_retweeted_post ON interactions(retweeted_post_id);
CREATE INDEX IF NOT EXISTS idx_interactions_quoted_post ON interactions(quoted_post_id);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_interactions_community_type ON interactions(community_id, interaction_type);
CREATE INDEX IF NOT EXISTS idx_interactions_author_type ON interactions(author_user_id, interaction_type);
CREATE INDEX IF NOT EXISTS idx_interactions_community_author ON interactions(community_id, author_user_id);

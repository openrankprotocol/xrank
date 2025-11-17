# Seed Graph Trust Score Generator

## Overview

This directory contains the script for generating local trust scores from seed user data. The trust scores are calculated based on various interaction types (follows, mentions, replies, retweets, quotes) extracted from seed user data files.

## Recent Changes

### Merged Trust Graph Output

The `generate_trust.py` script has been modified to:

1. **Process ALL seed data files**: The script now automatically discovers and processes all seed files matching these patterns in the `raw/` directory:
   - `*_seed_followings.json`
   - `*_seed_extended_followings.json`
   - `*_seed_interactions.json`

2. **Deduplicate data**: Duplicate data across seed files is now properly handled:
   - Follow relationships are tracked by `(source, target)` pairs - each unique follow is counted only once
   - Posts and replies are tracked by `post_id` - the same post appearing in multiple seed files is processed only once
   - This prevents inflated trust scores from duplicate data

3. **Single merged output**: All local trust scores from all seed users are now merged into a single file:
   - Output file: `trust/seed_graph.csv`
   - Format: CSV with header `i,j,v` where:
     - `i` = source username (normalized, lowercase, no @)
     - `j` = target username (normalized, lowercase, no @)
     - `v` = aggregated trust score

### Deduplication Strategy

- **Follow relationships**: A follow from user A to user B is counted once, even if it appears in multiple seed files
- **Posts/Replies**: Each post (identified by `post_id`) is processed only once, regardless of how many seed files contain it
- **Interactions**: All interactions from a post (mentions, replies, retweets, quotes) are extracted, but the same post won't be processed multiple times

## Usage

Run the script from the project root directory:

```bash
cd xrank
python seed_graph/generate_trust.py
```

The script will:
1. Load configuration from `config.toml`
2. Discover all seed data files in `raw/`
3. Process each seed user's data with deduplication
4. Aggregate trust scores for all unique (i,j) pairs
5. Save the merged result to `trust/seed_graph.csv`

## Trust Weights

Trust weights are configured in `config.toml` under the `[trust_weights]` section:

```toml
[trust_weights]
follow = 30
mention = 30
reply = 20
retweet = 50
quote = 40
```

## Output Statistics

The script provides detailed statistics during execution:
- Number of seed users processed
- Number of unique follow relationships discovered
- Number of unique posts/replies processed
- Breakdown of interaction types (follow, mention, reply, retweet, quote)
- Total unique trust relationships in the final output
- Trust score statistics (min, max, average, total)

## File Structure

```
seed_graph/
├── generate_trust.py    # Main script for generating trust scores
└── README.md           # This file

../raw/                 # Input: Seed data files
├── {user_id}_seed_followings.json
├── {user_id}_seed_extended_followings.json
└── {user_id}_seed_interactions.json

../trust/              # Output: Trust scores
└── seed_graph.csv    # Merged trust graph from all seeds
```

## Notes

- Unlike community trust scores, seed graph scores do NOT apply a 2x weight multiplier, as there is no concept of "community posts" in the seed graph context
- All usernames are normalized (lowercase, @ symbol removed) for consistency
- Self-loops (i == j) are excluded from the trust matrix
- The script handles missing files gracefully and continues processing available data
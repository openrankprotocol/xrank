#!/usr/bin/env python3
"""
Seed Graph Trust Score Generator

This script generates local trust scores between users based on seed interactions:
1. Loads ALL *_seed_extended_followings.json, *_seed_followings.json, and *_seed_interactions.json from raw/
2. Creates local trust scores from one user to another based on interactions
3. Assigns weights to each interaction type based on config.toml trust_weights
4. Deduplicates data across seed files (same posts, followings, etc. only counted once)
5. Aggregates scores for each unique i,j pair
6. Saves merged local trust to trust/seed_graph.csv with header i,j,v

Interaction types and their sources:
- follow: from seed_followings.json (only seed users -> master_list) and seed_extended_followings.json (only towards master_list)
- mention: from seed_interactions.json (posts mentioning other users)
- reply: from seed_interactions.json (reply posts and replies list)
- retweet: from seed_interactions.json (retweet posts)
- quote: from seed_interactions.json (quote posts)

Seed Graph Follow Relationships:
- seed_followings.json: Only creates follow relationships from seed users TO master_list users
- seed_extended_followings.json: Only creates follow relationships towards master_list users

Deduplication Strategy:
- Follow relationships: tracked by (source, target) pair
- Posts/Replies: tracked by post_id to avoid counting same post multiple times
- Same interaction from duplicate data is only counted once

Note: Unlike the community version, this does NOT apply 2x weight multiplier for any posts,
as there is no concept of "community posts" in the seed graph context.
"""

import csv
import glob
import json
import os
import re
from collections import defaultdict
from datetime import datetime

import toml


def load_config():
    """Load configuration from config.toml"""
    try:
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Config is in the parent directory
        config_path = os.path.join(script_dir, "..", "config.toml")
        with open(config_path, "r") as f:
            config = toml.load(f)
            print("✓ Configuration loaded successfully")
            return config
    except FileNotFoundError:
        print("❌ Error: config.toml not found")
        return None
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return None


def get_seed_user_ids_from_config():
    """Get all seed user IDs from config.toml [seed_graph] section."""
    config = load_config()
    if not config:
        return set()

    seed_graph_config = config.get("seed_graph", {})
    seed_user_ids = set()
    for community_name, user_ids in seed_graph_config.items():
        if isinstance(user_ids, list):
            seed_user_ids.update(str(uid) for uid in user_ids)

    return seed_user_ids


def load_json_file(file_path):
    """Load data from a JSON file"""
    if not os.path.exists(file_path):
        print(f"⚠️  File not found: {file_path}")
        return None

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"✓ Loaded {os.path.basename(file_path)}")
        return data
    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return None


def normalize_username(username):
    """Normalize username by removing @ and converting to lowercase"""
    if not username:
        return ""
    return username.lower().strip().lstrip("@")


def normalize_user_id(user_id):
    """Normalize user_id to string format"""
    if not user_id:
        return ""
    return str(user_id).strip()


def build_username_to_id_map(
    followings_data, extended_followings_data, interactions_data
):
    """Build a mapping from normalized username to user_id from all data sources"""
    username_to_id = {}

    # From followings data - master_list and seed_users
    if followings_data:
        for user in followings_data.get("master_list", []):
            username = normalize_username(user.get("username", ""))
            user_id = normalize_user_id(user.get("user_id", ""))
            if username and user_id:
                username_to_id[username] = user_id

        for user in followings_data.get("seed_users", []):
            username = normalize_username(user.get("username", ""))
            user_id = normalize_user_id(user.get("user_id", ""))
            if username and user_id:
                username_to_id[username] = user_id

    # From extended followings data
    if extended_followings_data:
        for user in extended_followings_data.get("users", []):
            username = normalize_username(user.get("username", ""))
            user_id = normalize_user_id(user.get("user_id", ""))
            if username and user_id:
                username_to_id[username] = user_id

    # From interactions data
    if interactions_data:
        for user in interactions_data.get("users", []):
            username = normalize_username(user.get("username", ""))
            user_id = normalize_user_id(user.get("user_id", ""))
            if username and user_id:
                username_to_id[username] = user_id

    return username_to_id


def extract_mentions(text):
    """Extract mentioned usernames from text"""
    if not text:
        return []

    # Find all @mentions in the text
    mentions = re.findall(r"@(\w+)", text)
    return [normalize_username(mention) for mention in mentions]


def process_seed_followings(
    followings_data, trust_weights, seen_follows, username_to_id
):
    """Process seed_followings.json to extract follow relationships towards master_list only

    Args:
        followings_data: The followings data structure
        trust_weights: Weight configuration
        seen_follows: Set of (source, target) tuples to track duplicate follows
        username_to_id: Mapping from username to user_id
    """
    interactions = []
    if not followings_data:
        return interactions

    follow_weight = trust_weights.get("follow", 30)
    print(f"  Processing seed_followings.json with weight {follow_weight}")

    # Get seed users and master list
    seed_users = followings_data.get("seed_users", [])
    master_list = followings_data.get("master_list", [])

    if not seed_users or not master_list:
        print(f"    No seed users or master list found")
        return interactions

    # Create follow relationships from seed users to master list users
    follow_count = 0
    for seed_user in seed_users:
        seed_user_id = normalize_user_id(seed_user.get("user_id", ""))
        if not seed_user_id:
            continue

        # Each seed user follows all users in master_list
        for master_user in master_list:
            master_user_id = normalize_user_id(master_user.get("user_id", ""))
            if master_user_id and seed_user_id != master_user_id:
                # Check for duplicates
                follow_pair = (seed_user_id, master_user_id)
                if follow_pair not in seen_follows:
                    seen_follows.add(follow_pair)
                    interactions.append(
                        {
                            "type": "follow",
                            "source": seed_user_id,
                            "target": master_user_id,
                            "weight": follow_weight,
                        }
                    )
                    follow_count += 1

    print(
        f"    Found {follow_count} unique follow relationships (seed users -> master_list)"
    )
    return interactions


def process_seed_extended_followings(
    seed_extended_data, trust_weights, master_user_ids=None, seen_follows=None
):
    """Process seed_extended_followings.json to extract follow relationships towards master_list only

    Args:
        seed_extended_data: The extended followings data structure
        trust_weights: Weight configuration
        master_user_ids: Set of master list user_ids to filter by
        seen_follows: Set of (source, target) tuples to track duplicate follows
    """
    interactions = []
    if seen_follows is None:
        seen_follows = set()
    if not seed_extended_data:
        return interactions

    follow_weight = trust_weights.get("follow", 30)
    print(f"  Processing seed_extended_followings.json with weight {follow_weight}")

    users = seed_extended_data.get("users", [])
    if not users:
        print(f"    No users found")
        return interactions

    # Build a set of valid user_ids from the users list
    valid_user_ids = set()
    for user in users:
        user_id = normalize_user_id(user.get("user_id", ""))
        if user_id:
            valid_user_ids.add(user_id)

    # If master_user_ids is provided, only include relationships towards those users
    filter_by_master = master_user_ids is not None
    if filter_by_master:
        print(
            f"    Filtering to only include relationships towards {len(master_user_ids)} master_list users"
        )

    follow_count = 0
    for user in users:
        follower_id = normalize_user_id(user.get("user_id", ""))
        if not follower_id:
            continue

        following_ids = user.get("following_ids", [])
        for followed_id in following_ids:
            followed_id_str = normalize_user_id(followed_id)
            if followed_id_str and follower_id != followed_id_str:
                # Only include if target is in master_list (when filtering is enabled)
                if filter_by_master and followed_id_str not in master_user_ids:
                    continue

                # Check for duplicates
                follow_pair = (follower_id, followed_id_str)
                if follow_pair not in seen_follows:
                    seen_follows.add(follow_pair)
                    interactions.append(
                        {
                            "type": "follow",
                            "source": follower_id,
                            "target": followed_id_str,
                            "weight": follow_weight,
                        }
                    )
                    follow_count += 1

    print(f"    Found {follow_count} unique follow relationships")
    return interactions


def process_seed_interactions(
    interactions_data, trust_weights, seen_posts=None, username_to_id=None
):
    """Process seed user interactions to extract various interaction types

    Args:
        interactions_data: The interactions data structure
        trust_weights: Weight configuration
        seen_posts: Set of post_ids to track duplicate posts/replies
        username_to_id: Mapping from username to user_id
    """
    interactions = []
    if seen_posts is None:
        seen_posts = set()
    if username_to_id is None:
        username_to_id = {}
    if not interactions_data or "users" not in interactions_data:
        return interactions

    mention_weight = trust_weights.get("mention", 30)
    reply_weight = trust_weights.get("reply", 20)
    retweet_weight = trust_weights.get("retweet", 50)
    quote_weight = trust_weights.get("quote", 40)

    print(f"  Processing seed interactions")
    print(
        f"    Weights: mention={mention_weight}, reply={reply_weight}, retweet={retweet_weight}, quote={quote_weight}"
    )
    print(f"    No weight multipliers applied (seed graph has no community concept)")

    interaction_counts = defaultdict(int)

    for user in interactions_data["users"]:
        user_id = normalize_user_id(user.get("user_id", ""))
        if not user_id:
            continue

        # Process posts
        posts = user.get("posts", [])
        for post in posts:
            post_id = post.get("post_id", "")

            # Skip if we've already seen this post
            if post_id and post_id in seen_posts:
                continue
            if post_id:
                seen_posts.add(post_id)

            post_text = post.get("text", "")
            is_reply = post.get("is_reply", False)
            is_retweet = post.get("is_retweet")
            is_quote = post.get("is_quote")
            reply_to_user_id = normalize_user_id(post.get("reply_to_user_id", ""))
            # Fallback to username lookup if reply_to_user_id not available
            if not reply_to_user_id:
                reply_to_username = normalize_username(
                    post.get("reply_to_username", "")
                )
                reply_to_user_id = username_to_id.get(reply_to_username, "")

            # Process retweets
            if is_retweet:
                original_creator_id = normalize_user_id(
                    post.get("original_post_creator_user_id", "")
                )
                # Fallback to username lookup
                if not original_creator_id:
                    original_creator_username = normalize_username(
                        post.get("original_post_creator_username", "")
                    )
                    original_creator_id = username_to_id.get(
                        original_creator_username, ""
                    )

                if original_creator_id and user_id != original_creator_id:
                    interactions.append(
                        {
                            "type": "retweet",
                            "source": user_id,
                            "target": original_creator_id,
                            "weight": retweet_weight,
                        }
                    )
                    interaction_counts["retweet"] += 1

            # Process quotes (is_quote can be a dict or boolean)
            elif is_quote:
                original_creator_id = normalize_user_id(
                    post.get("original_post_creator_user_id", "")
                )
                # Fallback to username lookup
                if not original_creator_id:
                    original_creator_username = normalize_username(
                        post.get("original_post_creator_username", "")
                    )
                    original_creator_id = username_to_id.get(
                        original_creator_username, ""
                    )

                if original_creator_id and user_id != original_creator_id:
                    interactions.append(
                        {
                            "type": "quote",
                            "source": user_id,
                            "target": original_creator_id,
                            "weight": quote_weight,
                        }
                    )
                    interaction_counts["quote"] += 1

            # Process replies
            elif is_reply and reply_to_user_id:
                if user_id != reply_to_user_id:
                    interactions.append(
                        {
                            "type": "reply",
                            "source": user_id,
                            "target": reply_to_user_id,
                            "weight": reply_weight,
                        }
                    )
                    interaction_counts["reply"] += 1

            # Process mentions in post text (lookup user_id from username)
            mentions = extract_mentions(post_text)
            for mentioned_username in mentions:
                mentioned_user_id = username_to_id.get(mentioned_username, "")
                if mentioned_user_id and user_id != mentioned_user_id:
                    interactions.append(
                        {
                            "type": "mention",
                            "source": user_id,
                            "target": mentioned_user_id,
                            "weight": mention_weight,
                        }
                    )
                    interaction_counts["mention"] += 1

        # Process replies (separate from posts in seed_interactions format)
        replies = user.get("replies", [])
        for reply in replies:
            reply_id = reply.get("post_id", "")

            # Skip if we've already seen this reply
            if reply_id and reply_id in seen_posts:
                continue
            if reply_id:
                seen_posts.add(reply_id)

            reply_text = reply.get("text", "")
            reply_to_user_id = normalize_user_id(reply.get("reply_to_user_id", ""))
            # Fallback to username lookup
            if not reply_to_user_id:
                reply_to_username = normalize_username(
                    reply.get("reply_to_username", "")
                )
                reply_to_user_id = username_to_id.get(reply_to_username, "")

            if reply_to_user_id and user_id != reply_to_user_id:
                interactions.append(
                    {
                        "type": "reply",
                        "source": user_id,
                        "target": reply_to_user_id,
                        "weight": reply_weight,
                    }
                )
                interaction_counts["reply"] += 1

            # Process mentions in reply text (lookup user_id from username)
            mentions = extract_mentions(reply_text)
            for mentioned_username in mentions:
                mentioned_user_id = username_to_id.get(mentioned_username, "")
                if mentioned_user_id and user_id != mentioned_user_id:
                    interactions.append(
                        {
                            "type": "mention",
                            "source": user_id,
                            "target": mentioned_user_id,
                            "weight": mention_weight,
                        }
                    )
                    interaction_counts["mention"] += 1

    for interaction_type, count in sorted(interaction_counts.items()):
        print(f"    Found {count} {interaction_type} interactions")

    return interactions


def aggregate_trust_scores(all_interactions):
    """Aggregate trust scores for unique i,j pairs"""
    trust_matrix = defaultdict(float)
    interaction_stats = defaultdict(int)

    print(f"  Aggregating {len(all_interactions)} total interactions")

    for interaction in all_interactions:
        source = interaction["source"]
        target = interaction["target"]
        weight = interaction["weight"]
        interaction_type = interaction["type"]

        if source and target and source != target:
            pair = (source, target)
            trust_matrix[pair] += weight
            interaction_stats[interaction_type] += 1

    print(f"  Interaction type breakdown:")
    for interaction_type, count in sorted(interaction_stats.items()):
        print(f"    {interaction_type}: {count}")

    print(f"  Unique trust relationships: {len(trust_matrix)}")
    return trust_matrix


def save_trust_matrix(trust_matrix, output_name, trust_dir):
    """Save trust matrix to CSV file with header i,j,v"""
    os.makedirs(trust_dir, exist_ok=True)
    filename = os.path.join(trust_dir, f"{output_name}.csv")

    # Sort pairs for consistent output
    sorted_pairs = sorted(trust_matrix.items(), key=lambda x: (x[0][0], x[0][1]))

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        # Write header
        writer.writerow(["i", "j", "v"])

        # Write data
        for (i, j), v in sorted_pairs:
            writer.writerow([i, j, v])

    print(f"✅ Trust matrix saved to: {filename}")
    print(f"📊 Total pairs: {len(sorted_pairs)}")

    # Show statistics
    if sorted_pairs:
        values = [v for (_, _), v in sorted_pairs]
        min_weight = min(values)
        max_weight = max(values)
        avg_weight = sum(values) / len(values)
        total_weight = sum(values)

        print(f"📈 Trust score statistics:")
        print(f"  - Min: {min_weight}")
        print(f"  - Max: {max_weight}")
        print(f"  - Average: {avg_weight:.2f}")
        print(f"  - Total: {total_weight:.2f}")

    return filename


def process_seed_graph(raw_data_dir, trust_dir, trust_weights):
    """Process all seed graph data files and merge into single trust graph"""
    print(f"\n{'=' * 60}")
    print(f"Processing Seed Graph - Merging All Seed Files")
    print(f"{'=' * 60}")

    # Get raw_data_dir and make it relative to project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, "..")

    # Make paths absolute
    if not os.path.isabs(raw_data_dir):
        raw_data_dir = os.path.join(project_root, raw_data_dir.lstrip("./"))
    if not os.path.isabs(trust_dir):
        trust_dir = os.path.join(project_root, trust_dir.lstrip("./"))

    # Get configured seed user IDs from config.toml
    configured_user_ids = get_seed_user_ids_from_config()
    if not configured_user_ids:
        print("❌ No seed user IDs found in config.toml [seed_graph] section")
        return None

    print(f"📋 Configured seed user IDs: {', '.join(sorted(configured_user_ids))}")

    # Find ALL seed_followings files
    pattern = os.path.join(raw_data_dir, "*_seed_followings.json")
    all_matching_files = glob.glob(pattern)

    # Filter to only include files for users in config.toml
    matching_files = []
    for file_path in all_matching_files:
        file_user_id = os.path.basename(file_path).split("_seed_followings.json")[0]
        if file_user_id in configured_user_ids:
            matching_files.append(file_path)

    if not matching_files:
        print(
            f"❌ No seed followings files found for configured users in: {raw_data_dir}"
        )
        print(f"   Looking for user IDs: {', '.join(sorted(configured_user_ids))}")
        print("Please run fetch_followings.py first to generate seed followings files.")
        return None

    print(
        f"Found {len(all_matching_files)} total seed file(s), processing {len(matching_files)} for configured users"
    )

    # Collect all seed user IDs from filtered files
    seed_user_ids = []
    for followings_file in matching_files:
        seed_user_id = os.path.basename(followings_file).split("_seed_followings.json")[
            0
        ]
        seed_user_ids.append(seed_user_id)
        print(f"  - Seed user ID: {seed_user_id}")

    print(f"\n🔄 Processing interactions from all seed users...")
    print(f"ℹ️  Deduplicating data across seed files...")

    # Process each data source
    all_interactions = []
    all_master_usernames = set()

    # Global deduplication trackers
    seen_follows = set()  # Track (source, target) pairs for follows
    seen_posts = set()  # Track post_ids to avoid duplicate posts

    # Process each seed user's data
    for seed_user_id in seed_user_ids:
        print(f"\n  Processing seed user: {seed_user_id}")

        # Define file paths using seed_user_id prefix
        interactions_file = os.path.join(
            raw_data_dir, f"{seed_user_id}_seed_interactions.json"
        )
        followings_file = os.path.join(
            raw_data_dir, f"{seed_user_id}_seed_followings.json"
        )
        extended_followings_file = os.path.join(
            raw_data_dir, f"{seed_user_id}_seed_extended_followings.json"
        )

        # Load all data files for this seed user
        interactions_data = load_json_file(interactions_file)
        followings_data = load_json_file(followings_file)
        extended_followings_data = load_json_file(extended_followings_file)

        # Build username to user_id mapping from all data sources
        username_to_id = build_username_to_id_map(
            followings_data, extended_followings_data, interactions_data
        )
        print(f"    Built username->user_id map with {len(username_to_id)} entries")

        # Build master_list user_ids for this seed user
        master_user_ids = None
        if followings_data and "master_list" in followings_data:
            master_list = followings_data.get("master_list", [])
            master_user_ids = {
                normalize_user_id(user.get("user_id", ""))
                for user in master_list
                if user.get("user_id")
            }
            all_master_usernames.update(master_user_ids)
            print(f"    Master list: {len(master_user_ids)} users")

        # Process followings (with deduplication)
        if followings_data:
            following_interactions = process_seed_followings(
                followings_data, trust_weights, seen_follows, username_to_id
            )
            all_interactions.extend(following_interactions)
            print(f"    Added {len(following_interactions)} unique follow interactions")

        # Process extended followings (with deduplication)
        if extended_followings_data:
            extended_following_interactions = process_seed_extended_followings(
                extended_followings_data, trust_weights, master_user_ids, seen_follows
            )
            all_interactions.extend(extended_following_interactions)
            print(
                f"    Added {len(extended_following_interactions)} unique extended follow interactions"
            )

        # Process interactions (with deduplication)
        if interactions_data:
            seed_interactions = process_seed_interactions(
                interactions_data, trust_weights, seen_posts, username_to_id
            )
            all_interactions.extend(seed_interactions)
            print(f"    Added {len(seed_interactions)} unique interaction records")

    print(f"\n  Combined master list: {len(all_master_usernames)} unique user IDs")
    print(f"  Total unique interactions collected: {len(all_interactions)}")
    print(f"  Deduplication stats:")
    print(f"    - Unique follow relationships: {len(seen_follows)}")
    print(f"    - Unique posts/replies processed: {len(seen_posts)}")

    if not all_interactions:
        print("⚠️  No interactions found for seed graph")
        return None

    # Aggregate trust scores
    trust_matrix = aggregate_trust_scores(all_interactions)

    if not trust_matrix:
        print("⚠️  No trust relationships calculated")
        return None

    # Always save to seed_graph.csv (merged output)
    output_name = "seed_graph"

    filename = save_trust_matrix(trust_matrix, output_name, trust_dir)
    return filename


def main():
    """Main function to generate trust scores for seed graph"""
    try:
        print("🔗 SEED GRAPH TRUST SCORE GENERATOR")
        print("=" * 50)

        # Load configuration
        config = load_config()
        if not config:
            return

        # Get configuration values - seed graph uses raw/seed subdirectory
        raw_data_dir_base = config.get("output", {}).get("raw_data_dir", "./raw")
        raw_data_dir = os.path.join(raw_data_dir_base, "seed")
        trust_weights = config.get("trust_weights", {})
        trust_dir = "./trust"

        print(f"📁 Raw data directory: {raw_data_dir}")
        print(f"📁 Trust output directory: {trust_dir}")
        print(f"⚖️  Trust weights: {trust_weights}")
        print(f"ℹ️  Note: No 2x multiplier applied (no community concept)")

        # Process seed graph
        try:
            filename = process_seed_graph(raw_data_dir, trust_dir, trust_weights)
            if not filename:
                print("❌ Failed to generate seed graph trust scores")
                return
        except Exception as e:
            print(f"❌ Error processing seed graph: {e}")
            import traceback

            traceback.print_exc()
            return

        # Final summary
        print(f"\n{'=' * 60}")
        print(f"🎉 SEED GRAPH TRUST SCORE GENERATION COMPLETE")
        print(f"{'=' * 60}")
        print(f"✅ Successfully processed all seed files")
        print(f"📁 Trust file saved in: {trust_dir}/")

        # Show generated file info
        if os.path.exists(filename):
            with open(filename, "r") as f:
                line_count = sum(1 for _ in f) - 1  # Subtract header
            filename_base = os.path.basename(filename)
            print(f"📄 Generated merged file:")
            print(f"  - {filename_base} ({line_count} trust relationships)")
            print(
                f"  - Combined data from {len(glob.glob(os.path.join(os.path.dirname(filename), '..', 'raw', '*_seed_followings.json')))} seed users with deduplication"
            )

    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

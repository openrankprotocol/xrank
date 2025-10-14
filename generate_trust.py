#!/usr/bin/env python3
"""
Trust Score Generator

This script generates local trust scores between users based on their interactions:
1. Loads members interactions, non-members interactions, following network, and comment graph from raw/
2. Creates local trust scores from one user to another based on interactions
3. Assigns weights to each interaction type based on config.toml trust_weights
4. Aggregates scores for each unique i,j pair
5. Saves local trust to trust/[community_id].csv with header i,j,v

Interaction types and their sources:
- follow: from following_network.json
- mention: from members/non_members interactions (posts mentioning other users)
- reply: from members/non_members interactions (reply posts) and comment_graph.json (comments are treated as replies)
- retweet: from members/non_members interactions (retweet posts)
- quote: from members/non_members interactions (quote posts)

Note: Comments from comment_graph.json are treated as reply interactions since they represent the same behavior.
"""

import json
import os
import toml
import csv
import re
from collections import defaultdict
from datetime import datetime

def load_config():
    """Load configuration from config.toml"""
    try:
        with open('config.toml', 'r') as f:
            config = toml.load(f)
            print("✓ Configuration loaded successfully")
            return config
    except FileNotFoundError:
        print("❌ Error: config.toml not found")
        return None
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return None

def load_json_file(file_path):
    """Load data from a JSON file"""
    if not os.path.exists(file_path):
        print(f"⚠️  File not found: {file_path}")
        return None

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
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
    return username.lower().strip().lstrip('@')

def extract_mentions(text):
    """Extract mentioned usernames from text"""
    if not text:
        return []

    # Find all @mentions in the text
    mentions = re.findall(r'@(\w+)', text)
    return [normalize_username(mention) for mention in mentions]

def process_following_network(following_data, trust_weights):
    """Process following network to extract follow relationships"""
    interactions = []
    if not following_data or 'following_network' not in following_data:
        return interactions

    follow_weight = trust_weights.get('follow', 30)
    print(f"  Processing following network with weight {follow_weight}")

    follow_count = 0
    for user in following_data['following_network']:
        follower = normalize_username(user.get('username', ''))
        if not follower:
            continue

        following_list = user.get('following', [])
        for followed_user in following_list:
            followed = normalize_username(followed_user)
            if followed and follower != followed:
                interactions.append({
                    'type': 'follow',
                    'source': follower,
                    'target': followed,
                    'weight': follow_weight
                })
                follow_count += 1

    print(f"    Found {follow_count} follow relationships")
    return interactions

def process_members_interactions(members_data, trust_weights):
    """Process member interactions to extract various interaction types"""
    interactions = []
    if not members_data or 'members_interactions' not in members_data:
        return interactions

    mention_weight = trust_weights.get('mention', 30)
    reply_weight = trust_weights.get('reply', 20)
    retweet_weight = trust_weights.get('retweet', 50)
    quote_weight = trust_weights.get('quote', 40)

    print(f"  Processing member interactions")
    print(f"    Weights: mention={mention_weight}, reply={reply_weight}, retweet={retweet_weight}, quote={quote_weight}")

    interaction_counts = defaultdict(int)

    for member in members_data['members_interactions']:
        member_username = normalize_username(member.get('username', ''))
        if not member_username:
            continue

        posts = member.get('posts', [])
        for post in posts:
            post_text = post.get('text', '')
            is_reply = post.get('is_reply', False)
            is_retweet = post.get('is_retweet', False)
            is_quote = post.get('is_quote', False)
            reply_to_username = normalize_username(post.get('reply_to_username', ''))

            # Process retweets
            if is_retweet:
                original_creator = normalize_username(post.get('original_post_creator_username', ''))
                if original_creator and member_username != original_creator:
                    interactions.append({
                        'type': 'retweet',
                        'source': member_username,
                        'target': original_creator,
                        'weight': retweet_weight
                    })
                    interaction_counts['retweet'] += 1

            # Process quotes
            elif is_quote:
                original_creator = normalize_username(post.get('original_post_creator_username', ''))
                if original_creator and member_username != original_creator:
                    interactions.append({
                        'type': 'quote',
                        'source': member_username,
                        'target': original_creator,
                        'weight': quote_weight
                    })
                    interaction_counts['quote'] += 1

            # Process replies
            elif is_reply and reply_to_username:
                if member_username != reply_to_username:
                    interactions.append({
                        'type': 'reply',
                        'source': member_username,
                        'target': reply_to_username,
                        'weight': reply_weight
                    })
                    interaction_counts['reply'] += 1

            # Process mentions in post text
            mentions = extract_mentions(post_text)
            for mentioned_user in mentions:
                if mentioned_user and member_username != mentioned_user:
                    interactions.append({
                        'type': 'mention',
                        'source': member_username,
                        'target': mentioned_user,
                        'weight': mention_weight
                    })
                    interaction_counts['mention'] += 1

    for interaction_type, count in interaction_counts.items():
        print(f"    Found {count} {interaction_type} interactions")

    return interactions

def process_non_members_interactions(non_members_data, trust_weights):
    """Process non-member interactions to extract various interaction types"""
    interactions = []
    if not non_members_data or 'non_member_interactions' not in non_members_data:
        return interactions

    mention_weight = trust_weights.get('mention', 30)
    reply_weight = trust_weights.get('reply', 20)
    retweet_weight = trust_weights.get('retweet', 50)
    quote_weight = trust_weights.get('quote', 40)

    print(f"  Processing non-member interactions")

    interaction_counts = defaultdict(int)

    for non_member in non_members_data['non_member_interactions']:
        non_member_username = normalize_username(non_member.get('username', ''))
        if not non_member_username:
            continue

        posts = non_member.get('posts', [])
        for post in posts:
            post_text = post.get('text', '')
            is_reply = post.get('is_reply', False)
            is_retweet = post.get('is_retweet', False)
            is_quote = post.get('is_quote', False)
            reply_to_username = normalize_username(post.get('reply_to_username', ''))

            # Process retweets
            if is_retweet:
                original_creator = normalize_username(post.get('original_post_creator_username', ''))
                if original_creator and non_member_username != original_creator:
                    interactions.append({
                        'type': 'retweet',
                        'source': non_member_username,
                        'target': original_creator,
                        'weight': retweet_weight
                    })
                    interaction_counts['retweet'] += 1

            # Process quotes
            elif is_quote:
                original_creator = normalize_username(post.get('original_post_creator_username', ''))
                if original_creator and non_member_username != original_creator:
                    interactions.append({
                        'type': 'quote',
                        'source': non_member_username,
                        'target': original_creator,
                        'weight': quote_weight
                    })
                    interaction_counts['quote'] += 1

            # Process replies
            elif is_reply and reply_to_username:
                if non_member_username != reply_to_username:
                    interactions.append({
                        'type': 'reply',
                        'source': non_member_username,
                        'target': reply_to_username,
                        'weight': reply_weight
                    })
                    interaction_counts['reply'] += 1

            # Process mentions in post text
            mentions = extract_mentions(post_text)
            for mentioned_user in mentions:
                if mentioned_user and non_member_username != mentioned_user:
                    interactions.append({
                        'type': 'mention',
                        'source': non_member_username,
                        'target': mentioned_user,
                        'weight': mention_weight
                    })
                    interaction_counts['mention'] += 1

    for interaction_type, count in interaction_counts.items():
        print(f"    Found {count} {interaction_type} interactions from non-members")

    return interactions

def build_user_lookup(members_data, non_members_data):
    """Build a lookup dictionary from user_id to username"""
    user_lookup = {}

    # Add members
    if members_data and 'members_interactions' in members_data:
        for member in members_data['members_interactions']:
            user_id = member.get('user_id', '')
            username = normalize_username(member.get('username', ''))
            if user_id and username:
                user_lookup[user_id] = username

    # Add non-members
    if non_members_data and 'non_member_interactions' in non_members_data:
        for non_member in non_members_data['non_member_interactions']:
            user_id = non_member.get('user_id', '')
            username = normalize_username(non_member.get('username', ''))
            if user_id and username:
                user_lookup[user_id] = username

    return user_lookup

def process_comment_graph(comment_data, trust_weights, user_lookup=None):
    """Process comment graph to extract comment interactions"""
    interactions = []
    if not comment_data or 'comment_graph' not in comment_data:
        return interactions

    reply_weight = trust_weights.get('reply', 20)  # Use reply weight for comments
    print(f"  Processing comment graph as replies with weight {reply_weight}")

    comment_count = 0
    skipped_count = 0

    for comment in comment_data['comment_graph']:
        commenter_username = normalize_username(comment.get('commenter_username', ''))
        original_author_username = normalize_username(comment.get('original_post_author_username', ''))

        # If commenter_username is empty, try to look it up by user_id
        if not commenter_username and user_lookup:
            commenter_user_id = comment.get('commenter_user_id', '')
            if commenter_user_id in user_lookup:
                commenter_username = user_lookup[commenter_user_id]

        # Skip if we still don't have both usernames or they're the same
        if not commenter_username or not original_author_username:
            skipped_count += 1
            continue

        if commenter_username != original_author_username:
            interactions.append({
                'type': 'reply',
                'source': commenter_username,
                'target': original_author_username,
                'weight': reply_weight
            })
            comment_count += 1

    print(f"    Found {comment_count} reply interactions from comments, skipped {skipped_count} due to missing usernames")
    return interactions

def aggregate_trust_scores(all_interactions):
    """Aggregate trust scores for unique i,j pairs"""
    trust_matrix = defaultdict(int)
    interaction_stats = defaultdict(int)

    print(f"  Aggregating {len(all_interactions)} total interactions")

    for interaction in all_interactions:
        source = interaction['source']
        target = interaction['target']
        weight = interaction['weight']
        interaction_type = interaction['type']

        if source and target and source != target:
            pair = (source, target)
            trust_matrix[pair] += weight
            interaction_stats[interaction_type] += 1

    print(f"  Interaction type breakdown:")
    for interaction_type, count in sorted(interaction_stats.items()):
        print(f"    {interaction_type}: {count}")

    print(f"  Unique trust relationships: {len(trust_matrix)}")
    return trust_matrix

def save_trust_matrix(trust_matrix, community_id, trust_dir):
    """Save trust matrix to CSV file with header i,j,v"""
    os.makedirs(trust_dir, exist_ok=True)
    filename = os.path.join(trust_dir, f"{community_id}.csv")

    # Sort pairs for consistent output
    sorted_pairs = sorted(trust_matrix.items(), key=lambda x: (x[0][0], x[0][1]))

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Write header
        writer.writerow(['i', 'j', 'v'])

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
        print(f"  - Total: {total_weight}")

    return filename

def process_community(community_id, raw_data_dir, trust_dir, trust_weights):
    """Process a single community to generate trust scores"""
    print(f"\n{'='*60}")
    print(f"Processing community: {community_id}")
    print(f"{'='*60}")

    # Define file paths
    members_file = os.path.join(raw_data_dir, f"{community_id}_members_interactions.json")
    non_members_file = os.path.join(raw_data_dir, f"{community_id}_non_member_interactions.json")
    following_file = os.path.join(raw_data_dir, f"{community_id}_following_network.json")
    comment_file = os.path.join(raw_data_dir, f"{community_id}_comment_graph.json")

    # Load all data files
    members_data = load_json_file(members_file)
    non_members_data = load_json_file(non_members_file)
    following_data = load_json_file(following_file)
    comment_data = load_json_file(comment_file)

    # Check if we have at least some data
    if not any([members_data, non_members_data, following_data, comment_data]):
        print("❌ No data files found for this community")
        return None

    print(f"🔄 Processing interactions...")

    # Process each data source
    all_interactions = []

    if following_data:
        following_interactions = process_following_network(following_data, trust_weights)
        all_interactions.extend(following_interactions)

    if members_data:
        members_interactions = process_members_interactions(members_data, trust_weights)
        all_interactions.extend(members_interactions)

    if non_members_data:
        non_members_interactions = process_non_members_interactions(non_members_data, trust_weights)
        all_interactions.extend(non_members_interactions)

    if comment_data:
        # Build user lookup for comment processing
        user_lookup = build_user_lookup(members_data, non_members_data)
        print(f"  Built user lookup with {len(user_lookup)} entries")
        comment_interactions = process_comment_graph(comment_data, trust_weights, user_lookup)
        all_interactions.extend(comment_interactions)

    if not all_interactions:
        print("⚠️  No interactions found for this community")
        return None

    # Aggregate trust scores
    trust_matrix = aggregate_trust_scores(all_interactions)

    if not trust_matrix:
        print("⚠️  No trust relationships calculated")
        return None

    # Save trust matrix
    filename = save_trust_matrix(trust_matrix, community_id, trust_dir)
    return filename

def main():
    """Main function to generate trust scores"""
    try:
        print("🔗 TRUST SCORE GENERATOR")
        print("=" * 50)

        # Load configuration
        config = load_config()
        if not config:
            return

        # Get configuration values
        community_ids = config['communities']['ids']
        raw_data_dir = config['output']['raw_data_dir']
        trust_weights = config.get('trust_weights', {})
        trust_dir = './trust'

        print(f"📁 Raw data directory: {raw_data_dir}")
        print(f"📁 Trust output directory: {trust_dir}")
        print(f"🏘️  Communities to process: {len(community_ids)}")
        print(f"⚖️  Trust weights: {trust_weights}")

        # Process each community
        processed_communities = 0
        for i, community_id in enumerate(community_ids):
            try:
                filename = process_community(community_id, raw_data_dir, trust_dir, trust_weights)
                if filename:
                    processed_communities += 1
            except Exception as e:
                print(f"❌ Error processing community {community_id}: {e}")
                import traceback
                traceback.print_exc()

        # Final summary
        print(f"\n{'='*60}")
        print(f"🎉 TRUST SCORE GENERATION COMPLETE")
        print(f"{'='*60}")
        print(f"✅ Successfully processed: {processed_communities}/{len(community_ids)} communities")
        print(f"📁 Trust files saved in: {trust_dir}/")

        # List generated files
        if os.path.exists(trust_dir):
            csv_files = [f for f in os.listdir(trust_dir) if f.endswith('.csv')]
            print(f"📄 Generated files:")
            for csv_file in sorted(csv_files):
                file_path = os.path.join(trust_dir, csv_file)
                if os.path.exists(file_path):
                    with open(file_path, 'r') as f:
                        line_count = sum(1 for _ in f) - 1  # Subtract header
                    print(f"  - {csv_file} ({line_count} trust relationships)")

    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

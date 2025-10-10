#!/usr/bin/env python3
"""
Trust Weight Generator

This script generates trust weights between users based on their interactions:
1. Loads internal and external interactions from JSON files
2. Calculates trust weights based on configurable interaction types
3. Aggregates weights for unique i,j pairs
4. Saves results to trust/{community_id}.csv

Trust weights (configurable in config.toml):
- mention: 30 points
- reply: 20 points
- retweet: 50 points
- quote: 40 points
"""

import json
import os
import toml
import csv
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

def load_interactions(file_path):
    """Load interactions from a JSON file"""
    if not os.path.exists(file_path):
        print(f"⚠️  File not found: {file_path}")
        return []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        interactions = data.get('interactions', [])

        # Handle different JSON structures
        if isinstance(interactions, dict):
            # External interactions format: {"mentions": [...], "replies": [...], etc.}
            all_interactions = []
            for interaction_type, interaction_list in interactions.items():
                for interaction in interaction_list:
                    # Set the type and normalize the receiving user field
                    interaction['type'] = interaction_type.rstrip('s')  # Remove plural 's'
                    # External interactions use 'receiving_user', normalize to 'target_user'
                    if 'receiving_user' in interaction:
                        interaction['target_user'] = interaction['receiving_user']
                    all_interactions.append(interaction)
            interactions = all_interactions
        else:
            # Internal interactions format: flat array with 'target_user' field
            # Already in correct format, just ensure consistent field names
            pass

        print(f"✓ Loaded {len(interactions)} interactions from {os.path.basename(file_path)}")
        return interactions

    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return []

def calculate_trust_weights(interactions, trust_weights_config):
    """Calculate trust weights from interactions"""
    trust_matrix = defaultdict(int)

    # Get trust weights from config
    weights = {
        'mention': trust_weights_config.get('mention', 30),
        'reply': trust_weights_config.get('reply', 20),
        'retweet': trust_weights_config.get('retweet', 50),
        'quote': trust_weights_config.get('quote', 40)
    }

    print(f"📊 Using trust weights: {weights}")

    interaction_counts = defaultdict(int)

    for interaction in interactions:
        interaction_type = interaction.get('type', '').lower()
        origin_user = interaction.get('origin_user', '').lower().strip()

        # Get target user (should be normalized to 'target_user' by load_interactions)
        target_user = interaction.get('target_user', '').lower().strip()

        if not origin_user or not target_user or origin_user == target_user:
            continue

        if interaction_type in weights:
            pair = (origin_user, target_user)
            weight = weights[interaction_type]
            trust_matrix[pair] += weight
            interaction_counts[interaction_type] += 1

    print(f"📈 Interaction counts: {dict(interaction_counts)}")
    print(f"🔗 Unique user pairs: {len(trust_matrix)}")

    return trust_matrix

def save_trust_matrix(trust_matrix, community_id, trust_dir):
    """Save trust matrix to CSV file"""

    # Create trust directory if it doesn't exist
    os.makedirs(trust_dir, exist_ok=True)

    # Create filename
    filename = os.path.join(trust_dir, f"{community_id}.csv")

    # Sort pairs for consistent output
    sorted_pairs = sorted(trust_matrix.items(), key=lambda x: (x[0][0], x[0][1]))

    # Write CSV file
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

        print(f"📈 Weight statistics:")
        print(f"  - Min: {min_weight}")
        print(f"  - Max: {max_weight}")
        print(f"  - Average: {avg_weight:.2f}")
        print(f"  - Total: {total_weight}")

    return filename

def process_community(community_id, raw_data_dir, trust_dir, trust_weights_config):
    """Process a single community to generate trust weights"""
    print(f"\n{'='*60}")
    print(f"Processing community: {community_id}")
    print(f"{'='*60}")

    # Load internal interactions
    internal_file = os.path.join(raw_data_dir, f"{community_id}_internal_interactions.json")
    internal_interactions = load_interactions(internal_file)

    # Load external interactions
    external_file = os.path.join(raw_data_dir, f"{community_id}_external_interactions.json")
    external_interactions = load_interactions(external_file)

    # Combine all interactions
    all_interactions = internal_interactions + external_interactions
    print(f"📊 Total interactions to process: {len(all_interactions)}")

    if not all_interactions:
        print("⚠️  No interactions found for this community")
        return None

    # Calculate trust weights
    print(f"🔄 Calculating trust weights...")
    trust_matrix = calculate_trust_weights(all_interactions, trust_weights_config)

    if not trust_matrix:
        print("⚠️  No trust relationships calculated")
        return None

    # Save trust matrix
    filename = save_trust_matrix(trust_matrix, community_id, trust_dir)

    return filename

def main():
    """Main function to generate trust weights"""
    try:
        print("🔗 TRUST WEIGHT GENERATOR")
        print("=" * 50)

        # Load configuration
        config = load_config()
        if not config:
            return

        # Get configuration values
        community_ids = config['communities']['ids']
        raw_data_dir = config['output']['raw_data_dir']
        trust_weights_config = config.get('trust_weights', {})

        # Create trust directory path
        trust_dir = './trust'

        print(f"📁 Raw data directory: {raw_data_dir}")
        print(f"📁 Trust output directory: {trust_dir}")
        print(f"🏘️  Communities to process: {len(community_ids)}")

        # Process each community
        processed_communities = 0
        for i, community_id in enumerate(community_ids):
            try:
                filename = process_community(community_id, raw_data_dir, trust_dir, trust_weights_config)
                if filename:
                    processed_communities += 1

            except Exception as e:
                print(f"❌ Error processing community {community_id}: {e}")
                import traceback
                traceback.print_exc()

        # Final summary
        print(f"\n{'='*60}")
        print(f"🎉 PROCESSING COMPLETE")
        print(f"{'='*60}")
        print(f"✅ Successfully processed: {processed_communities}/{len(community_ids)} communities")
        print(f"📁 Trust files saved in: {trust_dir}/")

        # List generated files
        if os.path.exists(trust_dir):
            csv_files = [f for f in os.listdir(trust_dir) if f.endswith('.csv')]
            print(f"📄 Generated files:")
            for csv_file in sorted(csv_files):
                print(f"  - {csv_file}")

    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Score Filter and Normalizer

This script processes score files from scores/ directory and creates filtered/normalized versions:
1. Loads scores from scores/[community_id].csv
2. Creates normalized version (0-100k scale) removing zero scores
3. Creates filtered version keeping only members and moderators from raw/[community_id]_members.json
4. Saves both versions to output/ directory

Output files:
- output/[community_id]_normalized.csv - All scores normalized 0-100k, zero scores removed
- output/[community_id]_members_and_mods.csv - Members and moderators, normalized 0-100k, zero scores removed
"""

import json
import os
import toml
import csv

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

def load_scores(scores_file):
    """Load scores from CSV file"""
    scores = {}

    if not os.path.exists(scores_file):
        print(f"⚠️  Scores file not found: {scores_file}")
        return scores

    try:
        with open(scores_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                username = row.get('i', '').strip()
                score_str = row.get('v', '').strip()
                if username and score_str:
                    try:
                        score = float(score_str)
                        scores[username.lower()] = score
                    except ValueError:
                        continue

        print(f"✓ Loaded {len(scores)} scores from {os.path.basename(scores_file)}")
        return scores
    except Exception as e:
        print(f"❌ Error loading scores from {scores_file}: {e}")
        return {}



def load_members_and_mods_list(members_file):
    """Load both member and moderator usernames from JSON file"""
    members_and_mods_set = set()

    if not os.path.exists(members_file):
        print(f"⚠️  Members file not found: {members_file}")
        return members_and_mods_set

    try:
        with open(members_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Load members
        if 'members' in data:
            for member in data['members']:
                username = member.get('username', '').strip().lower()
                if username:
                    members_and_mods_set.add(username)

        # Load moderators
        if 'moderators' in data:
            for moderator in data['moderators']:
                username = moderator.get('username', '').strip().lower()
                if username:
                    members_and_mods_set.add(username)

        print(f"✓ Loaded {len(members_and_mods_set)} members and moderators from {os.path.basename(members_file)}")
        return members_and_mods_set

    except Exception as e:
        print(f"❌ Error loading members and moderators from {members_file}: {e}")
        return set()

def normalize_scores(scores, target_max=100000):
    """Normalize scores to 0-target_max range, removing zero scores"""
    if not scores:
        return {}

    # Remove zero scores first
    non_zero_scores = {k: v for k, v in scores.items() if v > 0}

    if not non_zero_scores:
        print("⚠️  No non-zero scores found")
        return {}

    # Find min and max values
    min_score = min(non_zero_scores.values())
    max_score = max(non_zero_scores.values())

    print(f"  Original score range: {min_score:.6f} - {max_score:.6f}")

    # Handle edge case where all scores are the same
    if max_score == min_score:
        normalized = {k: target_max for k in non_zero_scores.keys()}
    else:
        # Normalize to 0-target_max range
        normalized = {}
        for username, score in non_zero_scores.items():
            normalized_score = ((score - min_score) / (max_score - min_score)) * target_max
            # Round to nearest integer and ensure minimum of 1 (no zeros after normalization)
            normalized_score = max(1, round(normalized_score))
            normalized[username] = normalized_score

    # Remove any that ended up as 0 after normalization (shouldn't happen but be safe)
    final_normalized = {k: v for k, v in normalized.items() if v > 0}

    print(f"  Normalized {len(final_normalized)} scores to range 1-{target_max}")

    return final_normalized

def save_scores_csv(scores, output_file):
    """Save scores to CSV file with header i,v"""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Sort by score descending for consistent output
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['i', 'v'])

        for username, score in sorted_scores:
            writer.writerow([username, score])

    print(f"✅ Saved {len(sorted_scores)} scores to {output_file}")

    # Show statistics
    if sorted_scores:
        values = [score for _, score in sorted_scores]
        min_score = min(values)
        max_score = max(values)
        avg_score = sum(values) / len(values)

        print(f"  📊 Score statistics:")
        print(f"    - Min: {min_score}")
        print(f"    - Max: {max_score}")
        print(f"    - Average: {avg_score:.1f}")

    return output_file

def filter_by_members(scores, members_set):
    """Filter scores to keep only members"""
    if not members_set:
        return scores

    filtered_scores = {k: v for k, v in scores.items() if k in members_set}

    print(f"  Filtered to {len(filtered_scores)} member scores (from {len(scores)} total)")

    return filtered_scores

def process_community(community_id, scores_dir, raw_data_dir, output_dir):
    """Process scores for a single community"""
    print(f"\n{'='*60}")
    print(f"Processing community: {community_id}")
    print(f"{'='*60}")

    # Define file paths
    scores_file = os.path.join(scores_dir, f"{community_id}.csv")
    members_file = os.path.join(raw_data_dir, f"{community_id}_members.json")

    # Load data
    scores = load_scores(scores_file)
    if not scores:
        print(f"❌ No scores found for community {community_id}")
        return None

    members_and_mods_set = load_members_and_mods_list(members_file)

    print(f"🔄 Processing scores...")

    # Create normalized version (all scores, normalized, zero scores removed)
    print("  Creating normalized version...")
    normalized_scores = normalize_scores(scores)

    if normalized_scores:
        normalized_file = os.path.join(output_dir, f"{community_id}_normalized.csv")
        save_scores_csv(normalized_scores, normalized_file)
    else:
        print("  ❌ Failed to create normalized scores")

    # Create members and moderators filtered version
    print("  Creating members and moderators filtered version...")
    members_and_mods_filtered_scores = filter_by_members(scores, members_and_mods_set)

    if members_and_mods_filtered_scores:
        members_and_mods_normalized = normalize_scores(members_and_mods_filtered_scores)

        if members_and_mods_normalized:
            members_and_mods_file = os.path.join(output_dir, f"{community_id}_members_and_mods.csv")
            save_scores_csv(members_and_mods_normalized, members_and_mods_file)
        else:
            print("  ❌ Failed to normalize members and moderators scores")
    else:
        print("  ⚠️  No member/moderator scores found to filter")

    return True

def main():
    """Main function to process score filtering and normalization"""
    try:
        print("📊 SCORE FILTER AND NORMALIZER")
        print("=" * 50)

        # Load configuration
        config = load_config()
        if not config:
            return

        # Get configuration values
        community_ids = config['communities']['ids']
        raw_data_dir = config['output']['raw_data_dir']

        # Define directories
        scores_dir = './scores'
        output_dir = './output'

        print(f"📁 Scores directory: {scores_dir}")
        print(f"📁 Raw data directory: {raw_data_dir}")
        print(f"📁 Output directory: {output_dir}")
        print(f"🏘️  Communities to process: {len(community_ids)}")

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Process each community
        processed_communities = 0
        for i, community_id in enumerate(community_ids):
            try:
                result = process_community(community_id, scores_dir, raw_data_dir, output_dir)
                if result:
                    processed_communities += 1
            except Exception as e:
                print(f"❌ Error processing community {community_id}: {e}")
                import traceback
                traceback.print_exc()

        # Final summary
        print(f"\n{'='*60}")
        print(f"🎉 SCORE PROCESSING COMPLETE")
        print(f"{'='*60}")
        print(f"✅ Successfully processed: {processed_communities}/{len(community_ids)} communities")
        print(f"📁 Filtered scores saved in: {output_dir}/")

        # List generated files
        if os.path.exists(output_dir):
            csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
            if csv_files:
                print(f"📄 Generated files:")
                for csv_file in sorted(csv_files):
                    file_path = os.path.join(output_dir, csv_file)
                    if os.path.exists(file_path):
                        with open(file_path, 'r') as f:
                            line_count = sum(1 for _ in f) - 1  # Subtract header
                        print(f"  - {csv_file} ({line_count} scores)")
            else:
                print("⚠️  No output files generated")

    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Score Processor

This script processes EigenTrust scores:
1. Loads scores/[seed_graph].csv
2. Passes scores through log2 function
3. Maps scores to 0.0-1.0 range (scores will be negative after log2)
4. Maps user IDs to usernames using raw/[seed_graph]_usernames.csv
5. Saves new scores to output/[seed_graph].csv (header: username,score)
"""

import argparse
import csv
import math
import os

import toml


def load_config():
    """Load configuration from config.toml"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "config.toml")
        with open(config_path, "r") as f:
            return toml.load(f)
    except FileNotFoundError:
        print("Error: config.toml not found")
        return None
    except Exception as e:
        print(f"Error loading config: {e}")
        return None


def load_scores(scores_dir, seed_graph_name):
    """Load scores from CSV file

    Args:
        scores_dir: Directory containing scores files
        seed_graph_name: Name of the seed graph

    Returns:
        List of (user_id, score) tuples
    """
    filename = os.path.join(scores_dir, f"{seed_graph_name}.csv")

    if not os.path.exists(filename):
        print(f"Error: {filename} not found")
        return None

    print(f"Loading {filename}...")

    scores = []
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row.get("i", "").strip()
            score_str = row.get("v", "").strip()
            if user_id and score_str:
                try:
                    score = float(score_str)
                    scores.append((user_id, score))
                except ValueError:
                    continue

    print(f"  Loaded {len(scores)} scores")
    return scores


def load_usernames(raw_data_dir, seed_graph_name):
    """Load username mappings from CSV file

    Args:
        raw_data_dir: Directory containing raw data files
        seed_graph_name: Name of the seed graph

    Returns:
        Dict mapping user_id -> username
    """
    filename = os.path.join(raw_data_dir, f"{seed_graph_name}_usernames.csv")

    if not os.path.exists(filename):
        print(f"Warning: {filename} not found, user IDs will be used instead")
        return {}

    print(f"Loading {filename}...")

    username_map = {}
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            username = row.get("username", "").strip()
            user_id = row.get("user_id", "").strip()
            if user_id and username:
                username_map[user_id] = username

    print(f"  Loaded {len(username_map)} usernames")
    return username_map


def process_scores(scores):
    """Process scores through log2 and normalize to 0.0-1.0 range

    Args:
        scores: List of (user_id, score) tuples

    Returns:
        List of (user_id, normalized_score) tuples
    """
    print("Processing scores...")

    # Apply log2 to all scores (filter out zero/negative scores)
    log_scores = []
    for user_id, score in scores:
        if score > 0:
            log_score = math.log2(score)
            log_scores.append((user_id, log_score))

    if not log_scores:
        print("  No valid scores after log2 transformation")
        return []

    # Find min and max for normalization
    min_score = min(s for _, s in log_scores)
    max_score = max(s for _, s in log_scores)

    print(f"  Log2 score range: {min_score:.4f} to {max_score:.4f}")

    # Normalize to 0.0-1.0 range
    score_range = max_score - min_score
    if score_range == 0:
        # All scores are the same
        normalized_scores = [(user_id, 0.5) for user_id, _ in log_scores]
    else:
        normalized_scores = [
            (user_id, (log_score - min_score) / score_range)
            for user_id, log_score in log_scores
        ]

    print(f"  Processed {len(normalized_scores)} scores")
    return normalized_scores


def save_output(scores, username_map, output_dir, seed_graph_name, use_user_ids=False):
    """Save processed scores to output CSV

    Args:
        scores: List of (user_id, score) tuples
        username_map: Dict mapping user_id -> username
        output_dir: Directory to save output
        seed_graph_name: Name of the seed graph
        use_user_ids: If True, output user IDs instead of usernames
    """
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"{seed_graph_name}.csv")

    # Sort by score descending
    sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)

    with open(filename, "w", encoding="utf-8") as f:
        if use_user_ids:
            f.write("user_id,score\n")
            for user_id, score in sorted_scores:
                f.write(f"{user_id},{score}\n")
        else:
            f.write("username,score\n")
            for user_id, score in sorted_scores:
                username = username_map.get(user_id, user_id)
                f.write(f"{username},{score}\n")

    print(f"✓ Saved {len(scores)} scores to: {filename}")


def main():
    """Main function - process scores and save output"""
    parser = argparse.ArgumentParser(description="Process EigenTrust scores")
    parser.add_argument(
        "--user-ids",
        action="store_true",
        help="Output user IDs instead of usernames",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Score Processor")
    print("=" * 60)

    # Load configuration
    config = load_config()
    if not config:
        return

    # Get seed graph name from config
    seed_graph_config = config.get("seed_graph", {})
    if not seed_graph_config:
        print("Error: No [seed_graph] section found in config.toml")
        return

    seed_graph_name = (
        list(seed_graph_config.keys())[0] if seed_graph_config else "unknown"
    )
    print(f"Using seed_graph: {seed_graph_name}")

    # Get directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_data_dir_config = config.get("output", {}).get("raw_data_dir", "./raw")
    raw_data_dir = os.path.join(script_dir, raw_data_dir_config.lstrip("./"))
    scores_dir = os.path.join(script_dir, "scores")
    output_dir = os.path.join(script_dir, "output")

    # Load scores
    scores = load_scores(scores_dir, seed_graph_name)
    if not scores:
        return

    # Load usernames
    username_map = load_usernames(raw_data_dir, seed_graph_name)

    # Process scores
    processed_scores = process_scores(scores)
    if not processed_scores:
        return

    # Save output
    save_output(
        processed_scores, username_map, output_dir, seed_graph_name, args.user_ids
    )

    # Summary
    print(f"\nSummary:")
    print(f"  Total scores: {len(processed_scores)}")
    if not args.user_ids:
        mapped_count = sum(
            1 for user_id, _ in processed_scores if user_id in username_map
        )
        print(f"  Mapped to usernames: {mapped_count}")
        print(f"  Using user IDs: {len(processed_scores) - mapped_count}")


if __name__ == "__main__":
    main()

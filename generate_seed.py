#!/usr/bin/env python3
"""
Seed CSV Generator

This script generates seed CSV files for EigenTrust:
1. Loads config.toml and extracts seed user IDs from [seed_graph] section
2. Reads interaction filenames in raw/ with format [seed_graph]_[user_id]_[user_id].json
3. Finds lowest and highest user IDs from these filenames
4. Filters seed IDs to only include those within the range
5. Saves CSV to seed/[seed_graph].csv with format: i,v where scores sum to 1.0
"""

import glob
import os
import re

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


def get_seed_graph_data(config):
    """Get seed graph names and their user IDs from config

    Returns:
        Dict mapping seed_graph_name -> list of user_id strings
    """
    seed_graph_config = config.get("seed_graph", {})
    result = {}

    for graph_name, user_ids in seed_graph_config.items():
        if isinstance(user_ids, list):
            # Convert all IDs to strings
            result[graph_name] = [str(uid) for uid in user_ids]

    return result


def get_interaction_file_range(raw_data_dir, seed_graph_name):
    """Get the lowest and highest user IDs from interaction filenames

    Args:
        raw_data_dir: Directory containing raw data files
        seed_graph_name: Name of the seed graph

    Returns:
        Tuple of (lowest_id, highest_id) as integers, or (None, None) if no files found
    """
    # Pattern: {seed_graph_name}_{id1}_{id2}.json
    pattern = os.path.join(raw_data_dir, f"{seed_graph_name}_*_*.json")
    matching_files = glob.glob(pattern)

    # Exclude followings and extended_followings files
    interaction_files = [
        f
        for f in matching_files
        if not f.endswith("_followings.json")
        and not f.endswith("_extended_followings.json")
    ]

    if not interaction_files:
        return None, None

    # Extract user IDs from filenames
    all_ids = []
    filename_pattern = re.compile(rf"{re.escape(seed_graph_name)}_(\d+)_(\d+)\.json$")

    for filepath in interaction_files:
        filename = os.path.basename(filepath)
        match = filename_pattern.match(filename)
        if match:
            id1 = int(match.group(1))
            id2 = int(match.group(2))
            all_ids.extend([id1, id2])

    if not all_ids:
        return None, None

    return min(all_ids), max(all_ids)


def filter_seed_ids(seed_ids, lowest_id, highest_id):
    """Filter seed IDs to only include those within the range

    Args:
        seed_ids: List of seed user ID strings
        lowest_id: Lowest user ID (inclusive)
        highest_id: Highest user ID (inclusive)

    Returns:
        List of filtered seed ID strings
    """
    filtered = []
    for uid in seed_ids:
        try:
            uid_int = int(uid)
            if lowest_id <= uid_int <= highest_id:
                filtered.append(uid)
        except ValueError:
            continue
    return filtered


def save_seed_csv(seed_ids, seed_dir, seed_graph_name):
    """Save seed CSV file with equal scores summing to 1.0

    Args:
        seed_ids: List of seed user ID strings
        seed_dir: Directory to save seed CSV
        seed_graph_name: Name of the seed graph

    Returns:
        Path to saved file
    """
    os.makedirs(seed_dir, exist_ok=True)
    filename = os.path.join(seed_dir, f"{seed_graph_name}.csv")

    if not seed_ids:
        # Write empty file with just header
        with open(filename, "w") as f:
            f.write("i,v\n")
        return filename

    # Calculate equal score for each seed user
    score = 1.0 / len(seed_ids)

    with open(filename, "w") as f:
        f.write("i,v\n")
        for uid in seed_ids:
            f.write(f"{uid},{score}\n")

    return filename


def main():
    """Main function - generate seed CSV files"""
    print("=" * 60)
    print("Seed CSV Generator")
    print("=" * 60)

    # Load configuration
    config = load_config()
    if not config:
        return

    # Get script directory for relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Get directories from config
    raw_data_dir_config = config.get("output", {}).get("raw_data_dir", "./raw")
    raw_data_dir = os.path.join(script_dir, raw_data_dir_config.lstrip("./"))

    seed_dir = os.path.join(script_dir, "seed")

    # Get seed graph data
    seed_graph_data = get_seed_graph_data(config)

    if not seed_graph_data:
        print("Error: No seed graph data found in config.toml [seed_graph] section")
        return

    print(f"Found {len(seed_graph_data)} seed graph(s) in config")
    print()

    for seed_graph_name, seed_ids in seed_graph_data.items():
        print(f"Processing: {seed_graph_name}")
        print(f"  Original seed IDs: {len(seed_ids)}")

        # Get range from interaction files
        lowest_id, highest_id = get_interaction_file_range(
            raw_data_dir, seed_graph_name
        )

        if lowest_id is None or highest_id is None:
            print(f"  ⚠️  No interaction files found for {seed_graph_name}")
            print(f"  Using all seed IDs without filtering")
            filtered_ids = seed_ids
        else:
            print(f"  Interaction file range: {lowest_id} - {highest_id}")

            # Filter seed IDs
            filtered_ids = filter_seed_ids(seed_ids, lowest_id, highest_id)
            print(f"  Filtered seed IDs: {len(filtered_ids)}")

            # Show which IDs were filtered out
            filtered_out = set(seed_ids) - set(filtered_ids)
            if filtered_out:
                print(f"  Filtered out {len(filtered_out)} IDs outside range")

        # Save CSV
        if filtered_ids:
            score = 1.0 / len(filtered_ids)
            print(f"  Score per user: {score:.10f}")

        filepath = save_seed_csv(filtered_ids, seed_dir, seed_graph_name)
        print(f"  ✓ Saved to: {filepath}")
        print()

    print("=" * 60)
    print("Seed CSV generation complete")
    print("=" * 60)


if __name__ == "__main__":
    main()

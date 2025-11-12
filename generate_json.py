#!/usr/bin/env python3
"""
Generate JSON files for UI from seed and output data.

This script:
1. Loads all entries from seed/ directory
2. Loads corresponding score files from output/ directory
3. Creates JSON files for each community with format:
   {
     "category": "xrank",
     "community_id": "<community_id>",
     "seed": [{"i": "username", "v": value}, ...],
     "scores": [{"i": "username", "v": value}, ...]
   }
4. Saves JSON files to ui/ directory

Usage:
    python3 generate_json.py

Requirements:
    - pandas (install with: pip install pandas)
    - CSV files in seed/ directory
    - CSV files in output/ directory (matching seed filenames with _users_log suffix)

Output:
    - Creates ui/ directory if it doesn't exist
    - For each seed file (e.g., bitcoin.csv), creates:
      - ui/bitcoin.json with seed and score data
"""

import json
import os
from pathlib import Path

import pandas as pd


def get_community_id(filename):
    """
    Get community ID from filename (already in correct format).

    Args:
        filename (str): Base filename without extension (community ID)

    Returns:
        str: Community ID
    """
    return filename


def load_seed_data(seed_file):
    """
    Load all entries from seed file.

    Args:
        seed_file (Path): Path to seed CSV file

    Returns:
        list: List of dictionaries with 'i' and 'v' keys
    """
    df = pd.read_csv(seed_file)

    # Convert to list of dictionaries
    seed_data = df.to_dict("records")

    return seed_data


def load_scores(scores_file):
    """
    Load scores from output file.

    Args:
        scores_file (Path): Path to scores CSV file

    Returns:
        list: List of dictionaries with 'i' and 'v' keys, or None if file not found
    """
    if not scores_file.exists():
        return None

    df = pd.read_csv(scores_file)

    # Convert to list of dictionaries
    scores_data = df.to_dict("records")

    return scores_data


def generate_json_file(community_id, seed_data, scores_data, output_file):
    """
    Generate JSON file with seed and scores data.

    Args:
        community_id (str): Community ID
        seed_data (list): List of seed entries
        scores_data (list): List of score entries
        output_file (Path): Path to output JSON file
    """
    json_data = {
        "category": "xrank",
        "community_id": community_id,
        "seed": seed_data,
        "scores": scores_data,
    }

    # Write JSON file with pretty formatting
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    print(f"✓ Created {output_file}")
    print(f"  Seed entries: {len(seed_data)}")
    print(f"  Score entries: {len(scores_data)}")


def main():
    """
    Main execution function.
    """
    print("=" * 60)
    print("Generating JSON files for UI")
    print("=" * 60)
    print()

    # Define directories
    seed_dir = Path("seed")
    output_dir = Path("output")
    ui_dir = Path("ui")

    # Create ui directory if it doesn't exist
    ui_dir.mkdir(exist_ok=True)
    print(f"✓ Output directory: {ui_dir}/")
    print()

    # Find all seed CSV files
    seed_files = sorted(list(seed_dir.glob("*.csv")))

    if not seed_files:
        print(f"❌ No CSV files found in {seed_dir}")
        return

    print(f"Found {len(seed_files)} seed file(s) to process...")
    print()

    # Process each seed file
    processed_count = 0
    skipped_count = 0

    for seed_file in seed_files:
        base_name = seed_file.stem
        community_id = get_community_id(base_name)

        print(f"Processing: {base_name}")

        # Load seed data (all entries)
        seed_data = load_seed_data(seed_file)

        # Find corresponding scores file (using _users_log suffix)
        scores_file = output_dir / f"{base_name}_users_log.csv"
        scores_data = load_scores(scores_file)

        # Skip if scores file not found
        if scores_data is None:
            print(f"⚠️  Skipping {base_name} - scores file not found: {scores_file}")
            skipped_count += 1
            print()
            continue

        # Generate JSON file
        output_file = ui_dir / f"{base_name}.json"
        generate_json_file(community_id, seed_data, scores_data, output_file)
        processed_count += 1
        print()

    print("=" * 60)
    print("✓ JSON generation complete!")
    print("=" * 60)
    print(
        f"\n✅ Successfully processed: {processed_count}/{len(seed_files)} communities"
    )
    if skipped_count > 0:
        print(f"⚠️  Skipped: {skipped_count} communities (missing scores files)")
    print(f"\nJSON files saved to {ui_dir}/")


if __name__ == "__main__":
    main()

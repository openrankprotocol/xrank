#!/usr/bin/env python3
"""
Score Processing Script

This script processes score files from the scores/ directory by:
1. Loading all CSV score files
2. Treating all peers as Discord users (usernames only)
3. Applying transformations to make exponential distributions more linear
4. Normalizing scores so all scores sum to 1
5. Saving results to output/ directory with transformation suffixes

Transformations available:
- Logarithmic (default): log transformation (first scaled to 1-100 range) to linearize exponential data
- Square root: sqrt transformation
- Quantile: quantile-based uniform distribution transformation

Usage:
    python3 process_scores.py              # Log transformation only (default)
    python3 process_scores.py --sqrt       # Sqrt transformation only
    python3 process_scores.py --quantile   # Quantile transformation only
    python3 process_scores.py --sqrt --quantile  # Both sqrt and quantile transformations
    python3 process_scores.py --members-only     # Filter to only community members (excludes mods/admins)
    python3 process_scores.py --members-and-mods # Filter to members, mods, and admins

Requirements:
    - pandas (install with: pip install pandas)
    - numpy (install with: pip install numpy)
    - scipy (for quantile transformation)
    - CSV files in scores/ directory with columns 'i' (identifier) and 'v' (score)

Output:
    - Creates output/ directory if it doesn't exist
    - For each input file (e.g., ai.csv), creates:
      - {filename}_users_log.csv: Users with logarithmic transformation (default, if no flags)
      - {filename}_users_sqrt.csv: Users with sqrt transformation (if --sqrt flag is used)
      - {filename}_users_quantile.csv: Users with quantile transformation (if --quantile flag is used)
    - Scores are normalized and mapped to 0-1000 range, sorted by score (descending)
"""

import argparse
import json
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats


def apply_sqrt_transformation(df):
    """Apply square root transformation to scores"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # Apply sqrt transformation
    df_transformed["v"] = np.sqrt(df["v"])

    # Normalize to 0-1 range
    min_val = df_transformed["v"].min()
    max_val = df_transformed["v"].max()
    if max_val != min_val:
        df_transformed["v"] = (df_transformed["v"] - min_val) / (max_val - min_val)
    else:
        df_transformed["v"] = 1.0 / len(df)

    # Map to 100-1000 range
    df_transformed["v"] = df_transformed["v"] * 1000

    # Round to 2 decimal places
    df_transformed["v"] = df_transformed["v"].round(2)

    return df_transformed


def apply_log_transformation(df):
    """Apply logarithmic transformation to scores (first scale to 1-100, then log)"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # First normalize to 0-1 range
    min_val = df["v"].min()
    max_val = df["v"].max()
    if max_val != min_val:
        df_transformed["v"] = (df["v"] - min_val) / (max_val - min_val)
    else:
        df_transformed["v"] = 1.0 / len(df)

    # Map to 1-100 range
    df_transformed["v"] = df_transformed["v"] * 99 + 1

    # Apply log transformation
    df_transformed["v"] = np.log(df_transformed["v"])

    # Normalize back to 0-1 range
    min_log = df_transformed["v"].min()
    max_log = df_transformed["v"].max()
    if max_log != min_log:
        df_transformed["v"] = (df_transformed["v"] - min_log) / (max_log - min_log)
    else:
        df_transformed["v"] = 1.0 / len(df)

    # Map to 100-1000 range
    df_transformed["v"] = df_transformed["v"] * 1000

    # Round to 2 decimal places
    df_transformed["v"] = df_transformed["v"].round(2)

    return df_transformed


def apply_quantile_transformation(df):
    """Apply quantile-based uniform distribution transformation"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # Use scipy for quantile transformation
    df_transformed["v"] = stats.rankdata(df["v"]) / len(df["v"])

    # Map to 100-1000 range
    df_transformed["v"] = df_transformed["v"] * 1000

    # Round to 2 decimal places
    df_transformed["v"] = df_transformed["v"].round(2)

    return df_transformed


def load_community_data(community_id, raw_dir="raw"):
    """
    Load community members and moderators from raw/[community_id]_members.json

    Args:
        community_id (str): Community ID
        raw_dir (str): Directory containing raw data files

    Returns:
        tuple: (set of member usernames, set of moderator usernames)
    """
    members_file = os.path.join(raw_dir, f"{community_id}_members.json")

    if not os.path.exists(members_file):
        print(f"    Warning: Members file not found: {members_file}")
        return set(), set()

    try:
        with open(members_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Extract usernames from members list
        members = data.get("members", [])
        member_usernames = {
            member["username"] for member in members if "username" in member
        }
        print(
            f"    Loaded {len(member_usernames)} community members from {members_file}"
        )

        # Extract usernames from moderators list
        moderators = data.get("moderators", [])
        moderator_usernames = {
            mod["username"] for mod in moderators if "username" in mod
        }
        print(f"    Loaded {len(moderator_usernames)} moderators from {members_file}")

        return member_usernames, moderator_usernames

    except Exception as e:
        print(f"    Error loading members file: {e}")
        return set(), set()


def process_scores(
    input_file,
    output_dir,
    include_sqrt=False,
    include_quantile=False,
    members_only=False,
    members_and_mods=False,
):
    """
    Process a single score file by applying transformations and saving

    Args:
        input_file (str): Path to input CSV file
        output_dir (str): Directory to save processed files
        include_sqrt (bool): Whether to include sqrt transformation
        include_quantile (bool): Whether to include quantile transformation
        members_only (bool): Whether to filter to only community members (excludes mods/admins)
        members_and_mods (bool): Whether to filter to members, mods, and admins
    """
    # Load the CSV file
    df = pd.read_csv(input_file)

    # Filter to community members if requested
    if members_only or members_and_mods:
        community_id = Path(input_file).stem
        member_usernames, moderator_usernames = load_community_data(community_id)

        if members_only:
            # Only include members, exclude moderators
            allowed_usernames = member_usernames - moderator_usernames
            filter_description = "members only (excluding mods/admins)"
        else:  # members_and_mods
            # Include both members and moderators
            allowed_usernames = member_usernames | moderator_usernames
            filter_description = "members and mods/admins"

        if allowed_usernames:
            original_count = len(df)
            df = df[df["i"].isin(allowed_usernames)]
            filtered_count = len(df)
            print(
                f"    Filtered to {filter_description}: {filtered_count}/{original_count} users"
            )

            # Normalize scores to sum to 1.0 after filtering
            if len(df) > 0:
                score_sum = df["v"].sum()
                if score_sum > 0:
                    df["v"] = df["v"] / score_sum
                    print(
                        f"    Normalized scores to sum to 1.0 (sum before: {score_sum:.6f})"
                    )
                else:
                    # If all scores are 0, assign equal weight
                    df["v"] = 1.0 / len(df)
                    print(
                        f"    All scores were 0, assigned equal weight: {df['v'].iloc[0]:.6f}"
                    )

    # Build transformations dict based on flags
    transformations = {}

    # If no flags are passed, default to log transformation
    if not include_sqrt and not include_quantile:
        transformations["log"] = apply_log_transformation
    else:
        # Use only the specified transformations
        if include_sqrt:
            transformations["sqrt"] = apply_sqrt_transformation

        if include_quantile:
            transformations["quantile"] = apply_quantile_transformation

    base_name = Path(input_file).stem
    print(f"Processing {input_file}:")

    for transform_name, transform_func in transformations.items():
        # Apply transformation to all users
        users_transformed = transform_func(df.copy())

        # Sort by score (descending)
        users_transformed = users_transformed.sort_values("v", ascending=False)

        # Generate output file name
        users_output = os.path.join(
            output_dir, f"{base_name}_users_{transform_name}.csv"
        )

        # Save the processed file
        users_transformed.to_csv(users_output, index=False)

        # Show score ranges
        users_min = users_transformed["v"].min() if len(users_transformed) > 0 else 0
        users_max = users_transformed["v"].max() if len(users_transformed) > 0 else 0

        print(f"  - {transform_name.capitalize()} transformation:")
        print(f"    Users: {len(users_transformed)} entries -> {users_output}")
        print(f"    Score range: {users_min:.2f} - {users_max:.2f}")


def main():
    """
    Main function to process all score files
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Process score files with transformations. Log transformation is applied by default when no flags are specified."
    )
    parser.add_argument(
        "--sqrt",
        action="store_true",
        help="Use square root transformation (replaces default log)",
    )
    parser.add_argument(
        "--quantile",
        action="store_true",
        help="Use quantile transformation (replaces default log)",
    )
    parser.add_argument(
        "--members-only",
        action="store_true",
        help="Filter scores to only include community members (excludes mods/admins) from raw/[community_id]_members.json",
    )
    parser.add_argument(
        "--members-and-mods",
        action="store_true",
        help="Filter scores to include both community members and mods/admins from raw/[community_id]_members.json",
    )
    args = parser.parse_args()

    # Define directories
    scores_dir = "scores"
    output_dir = "output"

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Find all CSV files in the scores directory
    scores_path = Path(scores_dir)
    csv_files = list(scores_path.glob("*.csv"))

    if not csv_files:
        print(f"No CSV files found in {scores_dir} directory")
        return

    # Show which transformations will be applied
    transformations = []
    if not args.sqrt and not args.quantile:
        transformations.append("log (default)")
    else:
        if args.sqrt:
            transformations.append("sqrt")
        if args.quantile:
            transformations.append("quantile")

    print(f"Transformations to apply: {', '.join(transformations)}")
    print(f"Found {len(csv_files)} score files to process...")
    print()

    # Show member filtering status
    if args.members_only:
        print(
            "Member filtering: ENABLED (only community members, excluding mods/admins)"
        )
    elif args.members_and_mods:
        print("Member filtering: ENABLED (community members and mods/admins)")
    else:
        print("Member filtering: DISABLED (all users will be included)")
    print()

    # Process each CSV file
    for csv_file in csv_files:
        try:
            process_scores(
                str(csv_file),
                output_dir,
                args.sqrt,
                args.quantile,
                args.members_only,
                args.members_and_mods,
            )
            print()
        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")
            print()

    print("Processing complete!")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Seed File Processor

This script processes seed files and assigns equal weights to all entries:
1. Loads all CSV files from seed/ directory
2. For each file, counts the number of entries (excluding header)
3. Assigns equal 'v' value to each 'i' such that all values sum to 1.0
4. Saves the updated seed files with normalized weights

File format: CSV with columns i,v
- i: identifier (username or user ID)
- v: weight value (normalized to sum to 1.0)
"""

import csv
import os
import sys
from pathlib import Path


def process_seed_file(filepath):
    """
    Process a single seed file to assign equal weights.

    Args:
        filepath: Path to the seed CSV file

    Returns:
        tuple: (filename, number of entries, success status)
    """
    try:
        # Read all entries
        entries = []
        with open(filepath, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                entries.append(row["i"])

        if not entries:
            print(f"⚠️  Warning: {os.path.basename(filepath)} has no entries")
            return (os.path.basename(filepath), 0, False)

        # Calculate equal weight for each entry
        num_entries = len(entries)
        equal_weight = 1.0 / num_entries

        # Write back with equal weights
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["i", "v"])

            for entry in entries:
                writer.writerow([entry, equal_weight])

        return (os.path.basename(filepath), num_entries, True)

    except Exception as e:
        print(f"❌ Error processing {os.path.basename(filepath)}: {e}")
        return (os.path.basename(filepath), 0, False)


def main():
    """Main function to process all seed files"""
    print("🌱 SEED FILE PROCESSOR")
    print("=" * 60)

    # Define seed directory
    seed_dir = "./seed"

    if not os.path.exists(seed_dir):
        print(f"❌ Error: Seed directory '{seed_dir}' not found")
        sys.exit(1)

    # Find all CSV files in seed directory
    csv_files = [f for f in os.listdir(seed_dir) if f.endswith(".csv")]

    if not csv_files:
        print(f"❌ Error: No CSV files found in '{seed_dir}'")
        sys.exit(1)

    print(f"📁 Seed directory: {seed_dir}")
    print(f"📄 Found {len(csv_files)} seed file(s) to process")
    print()

    # Process each seed file
    results = []
    for csv_file in sorted(csv_files):
        filepath = os.path.join(seed_dir, csv_file)
        print(f"Processing: {csv_file}...", end=" ")

        filename, num_entries, success = process_seed_file(filepath)
        results.append((filename, num_entries, success))

        if success:
            weight = 1.0 / num_entries if num_entries > 0 else 0
            print(f"✅ {num_entries} entries (weight: {weight:.6f} each)")
        else:
            print("❌ Failed")

    # Summary
    print()
    print("=" * 60)
    print("📊 PROCESSING SUMMARY")
    print("=" * 60)

    successful = sum(1 for _, _, success in results if success)
    total_entries = sum(num for _, num, success in results if success)

    print(f"✅ Successfully processed: {successful}/{len(csv_files)} files")
    print(f"📝 Total entries processed: {total_entries}")
    print()

    # Detailed summary
    if results:
        print("Detailed results:")
        for filename, num_entries, success in results:
            status = "✅" if success else "❌"
            if success and num_entries > 0:
                weight = 1.0 / num_entries
                print(f"  {status} {filename}: {num_entries} entries (v={weight:.6f})")
            else:
                print(f"  {status} {filename}: Failed or empty")

    print()
    print("🎉 Seed processing complete!")


if __name__ == "__main__":
    main()

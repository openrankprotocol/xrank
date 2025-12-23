#!/usr/bin/env python3
"""
Import Seed Graph Scores to Database

This script imports seed graph scores from scores/ and seed/ directories
into the xrank_seed PostgreSQL tables.

Imports data from CSV files:
- scores/{community_id}.csv -> xrank_seed.runs, xrank_seed.scores tables
- seed/{community_id}.csv -> xrank_seed.seeds table

Usage:
    python3 seed_graph/import_scores_to_db.py                    # Import all seed graphs from config
    python3 seed_graph/import_scores_to_db.py --community base_latam  # Import specific community
    python3 seed_graph/import_scores_to_db.py --dry-run          # Show what would be imported

Requirements:
    - psycopg2 (install with: pip install psycopg2-binary)
    - Environment variable: DATABASE_URL
"""

import argparse
import csv
import os
import sys
from pathlib import Path

import psycopg2
import toml
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# Load environment variables from .env file
load_dotenv()


def load_config():
    """Load configuration from config.toml"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "..", "config.toml")
        with open(config_path, "r") as f:
            return toml.load(f)
    except FileNotFoundError:
        print("❌ Error: config.toml not found")
        return None
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return None


def get_community_ids_from_config():
    """Get all community IDs (community names) from config.toml [seed_graph] section."""
    config = load_config()
    if not config:
        return []

    seed_graph_config = config.get("seed_graph", {})
    return list(seed_graph_config.keys())


def get_db_connection():
    """Get database connection from DATABASE_URL environment variable."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    return psycopg2.connect(database_url)


def ensure_schema_exists(conn, dry_run: bool = False):
    """Ensure the xrank_seed schema exists."""
    if dry_run:
        return

    cursor = conn.cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS xrank_seed")
        conn.commit()
    finally:
        cursor.close()


def create_tables_if_not_exist(conn, dry_run: bool = False):
    """Create the xrank_seed tables if they don't exist."""
    if dry_run:
        return

    cursor = conn.cursor()
    try:
        # Create runs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS xrank_seed.runs (
                community_id TEXT NOT NULL,
                run_id INTEGER NOT NULL,
                days_back INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (community_id, run_id)
            )
        """)

        # Create scores table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS xrank_seed.scores (
                community_id TEXT NOT NULL,
                run_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (community_id, run_id, user_id),
                FOREIGN KEY (community_id, run_id) REFERENCES xrank_seed.runs(community_id, run_id) ON DELETE CASCADE
            )
        """)

        # Create seeds table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS xrank_seed.seeds (
                community_id TEXT NOT NULL,
                run_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (community_id, run_id, user_id),
                FOREIGN KEY (community_id, run_id) REFERENCES xrank_seed.runs(community_id, run_id) ON DELETE CASCADE
            )
        """)

        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_seed_runs_community ON xrank_seed.runs(community_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_seed_scores_community_run ON xrank_seed.scores(community_id, run_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_seed_scores_score ON xrank_seed.scores(score DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_seed_seeds_community_run ON xrank_seed.seeds(community_id, run_id)
        """)

        conn.commit()
        print("✅ Schema and tables created/verified")
    finally:
        cursor.close()


def create_run(conn, community_id: str, days_back: int, dry_run: bool = False):
    """Create a new run entry and return the run_id (per-community incrementing)."""
    if dry_run:
        print(f"  🔍 Dry run - would create run for community {community_id}")
        return 1  # Return dummy run_id for dry run

    cursor = conn.cursor()
    try:
        # Get next run_id for this community
        cursor.execute(
            """
            SELECT COALESCE(MAX(run_id), 0) + 1
            FROM xrank_seed.runs
            WHERE community_id = %s
            """,
            (community_id,),
        )
        run_id = cursor.fetchone()[0]

        # Insert new run
        cursor.execute(
            """
            INSERT INTO xrank_seed.runs (community_id, run_id, days_back)
            VALUES (%s, %s, %s)
            """,
            (community_id, run_id, days_back),
        )
        conn.commit()
        print(f"  ✅ Created run {run_id} for community {community_id}")
        return run_id
    finally:
        cursor.close()


def process_scores_file(
    conn, file_path: Path, community_id: str, run_id: int, dry_run: bool = False
):
    """Process a scores CSV file and import into xrank_seed.scores table."""
    print(f"  📂 Loading scores from: {file_path.name}")

    scores_data = []
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row.get("i")
            score = row.get("v")
            if user_id and score:
                try:
                    scores_data.append(
                        (community_id, run_id, int(user_id), float(score))
                    )
                except ValueError:
                    # Skip rows with invalid user_id or score
                    continue

    print(f"  📊 Found {len(scores_data)} scores")

    if dry_run:
        print(f"  🔍 Dry run - no data inserted")
        return len(scores_data)

    cursor = conn.cursor()

    try:
        if scores_data:
            print(f"  💾 Inserting scores...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank_seed.scores (community_id, run_id, user_id, score)
                VALUES %s
                ON CONFLICT (community_id, run_id, user_id) DO UPDATE SET
                    score = EXCLUDED.score
                """,
                scores_data,
                page_size=1000,
            )

        conn.commit()
        print(f"  ✅ Imported {len(scores_data)} scores")

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error importing scores: {e}")
        raise
    finally:
        cursor.close()

    return len(scores_data)


def process_seed_file(
    conn, file_path: Path, community_id: str, run_id: int, dry_run: bool = False
):
    """Process a seed CSV file and import into xrank_seed.seeds table."""
    print(f"  📂 Loading seeds from: {file_path.name}")

    seed_data = []
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row.get("i")
            score = row.get("v")
            if user_id and score:
                try:
                    seed_data.append((community_id, run_id, int(user_id), float(score)))
                except ValueError:
                    # Skip rows with invalid user_id or score
                    continue

    print(f"  📊 Found {len(seed_data)} seed users")

    if dry_run:
        print(f"  🔍 Dry run - no data inserted")
        return len(seed_data)

    cursor = conn.cursor()

    try:
        if seed_data:
            print(f"  💾 Inserting seed users...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank_seed.seeds (community_id, run_id, user_id, score)
                VALUES %s
                ON CONFLICT (community_id, run_id, user_id) DO UPDATE SET
                    score = EXCLUDED.score
                """,
                seed_data,
                page_size=1000,
            )

        conn.commit()
        print(f"  ✅ Imported {len(seed_data)} seed users")

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error importing seeds: {e}")
        raise
    finally:
        cursor.close()

    return len(seed_data)


def import_seed_graph(
    conn, community_id: str, project_root: str, days_back: int, dry_run: bool = False
):
    """Import scores and seeds for a specific community."""
    print(f"\n{'=' * 50}")
    print(f"📊 Importing community: {community_id}")
    print(f"{'=' * 50}")

    scores_dir = os.path.join(project_root, "scores")
    seed_dir = os.path.join(project_root, "seed")

    # Check for scores file (could be seed_graph.csv or {community_id}.csv)
    scores_file = None
    for filename in [f"{community_id}.csv", "seed_graph.csv"]:
        potential_path = Path(scores_dir) / filename
        if potential_path.exists():
            scores_file = potential_path
            break

    # Check for seed file
    seed_file = None
    for filename in [f"{community_id}.csv", "seed_graph.csv"]:
        potential_path = Path(seed_dir) / filename
        if potential_path.exists():
            seed_file = potential_path
            break

    if not scores_file and not seed_file:
        print(f"  ⚠️  No scores or seed files found for {community_id}")
        print(f"      Looked in: {scores_dir} and {seed_dir}")
        return

    # Create a new run
    run_id = create_run(conn, community_id, days_back, dry_run)

    total_scores = 0
    total_seeds = 0

    # Import scores
    if scores_file:
        total_scores = process_scores_file(
            conn, scores_file, community_id, run_id, dry_run
        )
    else:
        print(f"  ⚠️  No scores file found")

    # Import seeds
    if seed_file:
        total_seeds = process_seed_file(conn, seed_file, community_id, run_id, dry_run)
    else:
        print(f"  ⚠️  No seed file found")

    print(f"\n  📈 Summary for {community_id}:")
    print(f"      Run ID: {run_id}")
    print(f"      Scores imported: {total_scores}")
    print(f"      Seeds imported: {total_seeds}")


def main():
    """Main function to import seed graph scores to database."""
    parser = argparse.ArgumentParser(
        description="Import seed graph scores from scores/ and seed/ to xrank_seed database tables"
    )
    parser.add_argument(
        "--community",
        type=str,
        help="Specific community ID to import (e.g., base_latam)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without inserting",
    )
    args = parser.parse_args()

    # Load config
    config = load_config()
    if not config:
        sys.exit(1)

    # Get days_back from config
    days_back = config.get("data", {}).get("days_back", 365)

    # Determine which communities to import
    if args.community:
        community_ids = [args.community]
    else:
        community_ids = get_community_ids_from_config()

    if not community_ids:
        print("❌ No community IDs found in config.toml [seed_graph] section")
        sys.exit(1)

    print(f"🔗 SEED GRAPH SCORES IMPORTER")
    print(f"{'=' * 50}")
    print(f"📋 Communities to import: {', '.join(community_ids)}")
    print(f"📅 Days back: {days_back}")

    if args.dry_run:
        print(f"🔍 DRY RUN MODE - No data will be inserted")

    # Get project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, "..")

    try:
        conn = get_db_connection()
        print(f"✅ Connected to database")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)

    try:
        # Ensure schema and tables exist
        ensure_schema_exists(conn, args.dry_run)
        create_tables_if_not_exist(conn, args.dry_run)

        # Import each community
        for community_id in community_ids:
            try:
                import_seed_graph(
                    conn, community_id, project_root, days_back, args.dry_run
                )
            except Exception as e:
                print(f"❌ Error importing {community_id}: {e}")
                import traceback

                traceback.print_exc()
                continue

        print(f"\n{'=' * 50}")
        print(f"🎉 IMPORT COMPLETE")
        print(f"{'=' * 50}")

    except Exception as e:
        print(f"\n❌ Error during import: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

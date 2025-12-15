#!/usr/bin/env python3
"""
Import Seed Interactions to Database

This script imports all seed interactions from raw/seed/*_seed_interactions.json
files into the xrank.interactions table with community_id = NULL.

It skips any posts where post_id already exists in the database.

Usage:
    python3 seed_graph/import_interactions_to_db.py                # Import all seed interactions
    python3 seed_graph/import_interactions_to_db.py --dry-run      # Show what would be imported

Requirements:
    - psycopg2 (install with: pip install psycopg2-binary)
    - Environment variable: DATABASE_URL
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
import toml
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# Load environment variables from .env file
load_dotenv()


def load_config():
    """Load configuration from config.toml"""
    try:
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Config is in the parent directory
        config_path = os.path.join(script_dir, "..", "config.toml")
        with open(config_path, "r") as f:
            return toml.load(f)
    except FileNotFoundError:
        print("Error: config.toml not found")
        return None
    except Exception as e:
        print(f"Error loading config: {e}")
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


def is_valid_numeric_id(value) -> bool:
    """Check if a value is a valid numeric ID (not a fallback like 'seed_username')."""
    if value is None:
        return False
    try:
        int(str(value))
        return True
    except ValueError:
        return False


def parse_twitter_date(date_str: Optional[str]) -> Optional[str]:
    """Parse Twitter date format to ISO format."""
    if not date_str:
        return None
    try:
        # Twitter format: "Tue Nov 25 13:32:14 +0000 2025"
        dt = datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")
        return dt.isoformat()
    except (ValueError, TypeError):
        return date_str  # Return as-is if already in a different format


def get_db_connection():
    """Get database connection from DATABASE_URL environment variable."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    return psycopg2.connect(database_url)


def get_existing_post_ids(conn) -> set:
    """Get all existing post_ids from the interactions table where community_id is NULL."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT post_id FROM xrank.interactions WHERE community_id IS NULL"
        )
        return {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()


def insert_interactions_with_null_community(cursor, interactions_data):
    """Insert interactions with NULL community_id, skipping duplicates.

    PostgreSQL's UNIQUE constraint doesn't work with NULL values (NULLs are not equal),
    so we need to check for existing post_ids manually before inserting.
    """
    if not interactions_data:
        return 0

    # Get existing post_ids with NULL community_id
    post_ids = [i[0] for i in interactions_data]

    # Query in batches to avoid too many parameters
    existing_ids = set()
    batch_size = 1000
    for i in range(0, len(post_ids), batch_size):
        batch = post_ids[i : i + batch_size]
        placeholders = ",".join(["%s"] * len(batch))
        cursor.execute(
            f"SELECT post_id FROM xrank.interactions WHERE community_id IS NULL AND post_id IN ({placeholders})",
            batch,
        )
        existing_ids.update(row[0] for row in cursor.fetchall())

    # Filter out existing interactions
    new_interactions = [i for i in interactions_data if i[0] not in existing_ids]

    if not new_interactions:
        return 0

    # Insert only new interactions
    execute_values(
        cursor,
        """
        INSERT INTO xrank.interactions (post_id, interaction_type, community_id, author_user_id,
            text, created_at, reply_to_post_id, reply_to_user_id,
            retweeted_post_id, retweeted_user_id, quoted_post_id, quoted_user_id)
        VALUES %s
        """,
        new_interactions,
        page_size=1000,
    )

    return len(new_interactions)


def process_seed_interactions_file(
    conn, file_path: Path, existing_post_ids: set, dry_run: bool = False
):
    """Process a seed interactions JSON file."""
    print(f"  📂 Loading seed interactions from: {file_path.name}")

    with open(file_path, "r") as f:
        data = json.load(f)

    users_data = data.get("users", [])

    # Count total interactions
    total_posts = sum(len(u.get("posts", [])) for u in users_data)
    total_replies = sum(len(u.get("replies", [])) for u in users_data)
    print(
        f"  📊 Found {len(users_data)} users with {total_posts} posts and {total_replies} replies"
    )

    if dry_run:
        print(f"  🔍 Dry run - no data inserted")
        return len(users_data), total_posts + total_replies, 0

    cursor = conn.cursor()

    try:
        # Prepare users and interactions
        users_to_insert = []
        interactions_to_insert = []
        seen_users = set()
        referenced_user_ids = set()
        skipped_count = 0

        for user in users_data:
            user_id = str(user.get("user_id")) if user.get("user_id") else None
            if not user_id or not is_valid_numeric_id(user_id):
                continue

            if user_id not in seen_users:
                users_to_insert.append(
                    (
                        int(user_id),
                        user.get("username", ""),
                        user.get("display_name"),
                    )
                )
                seen_users.add(user_id)

            # Process posts
            for post in user.get("posts", []):
                post_id = post.get("post_id")
                if not post_id:
                    continue

                # Skip if post already exists
                if int(post_id) in existing_post_ids:
                    skipped_count += 1
                    continue

                # Skip posts with non-numeric author IDs
                post_author = post.get("user_id", user_id)
                if not is_valid_numeric_id(post_author):
                    continue

                # Determine interaction type
                if post.get("is_retweet"):
                    interaction_type = "retweet"
                elif post.get("is_quote"):
                    interaction_type = "quote"
                elif post.get("is_reply"):
                    interaction_type = "reply"
                else:
                    interaction_type = "post"

                # Extract quoted post info
                quoted_post_id = post.get("quoted_post_id")
                quoted_user_id = None
                if isinstance(post.get("is_quote"), dict):
                    quoted_post_id = post["is_quote"].get("id")
                if post.get("original_post_creator_id"):
                    quoted_user_id = post.get("original_post_creator_id")

                # Collect author_user_id if different from user
                post_author_id = (
                    str(post.get("user_id")) if post.get("user_id") else user_id
                )
                if (
                    post_author_id
                    and post_author_id not in seen_users
                    and is_valid_numeric_id(post_author_id)
                ):
                    referenced_user_ids.add(post_author_id)

                # Collect referenced user IDs for placeholder records
                reply_to_uid = (
                    str(post.get("reply_to_user_id"))
                    if post.get("reply_to_user_id")
                    else None
                )
                if (
                    reply_to_uid
                    and reply_to_uid not in seen_users
                    and is_valid_numeric_id(reply_to_uid)
                ):
                    referenced_user_ids.add(reply_to_uid)

                original_creator_id = (
                    str(post.get("original_post_creator_id"))
                    if post.get("original_post_creator_id")
                    else None
                )
                if (
                    post.get("is_retweet")
                    and original_creator_id
                    and original_creator_id not in seen_users
                    and is_valid_numeric_id(original_creator_id)
                ):
                    referenced_user_ids.add(original_creator_id)

                quoted_uid = str(quoted_user_id) if quoted_user_id else None
                if (
                    quoted_uid
                    and quoted_uid not in seen_users
                    and is_valid_numeric_id(quoted_uid)
                ):
                    referenced_user_ids.add(quoted_uid)

                interactions_to_insert.append(
                    (
                        int(post_id),
                        interaction_type,
                        None,  # community_id is always NULL for seed graph
                        int(post.get("user_id", user_id)),
                        post.get("text"),
                        parse_twitter_date(post.get("created_at")),
                        int(post.get("reply_to_post_id"))
                        if post.get("reply_to_post_id")
                        else None,
                        int(post.get("reply_to_user_id"))
                        if post.get("reply_to_user_id")
                        else None,
                        int(post.get("retweeted_post_id"))
                        if post.get("retweeted_post_id")
                        else None,
                        int(post.get("original_post_creator_id"))
                        if post.get("is_retweet")
                        and post.get("original_post_creator_id")
                        else None,
                        int(quoted_post_id) if quoted_post_id else None,
                        int(quoted_user_id) if quoted_user_id else None,
                    )
                )

            # Process replies
            for reply in user.get("replies", []):
                post_id = reply.get("post_id")
                if not post_id:
                    continue

                # Skip replies with non-numeric author IDs
                reply_author = reply.get("user_id", user_id)
                if not is_valid_numeric_id(reply_author):
                    continue

                # Skip if post already exists
                if int(post_id) in existing_post_ids:
                    skipped_count += 1
                    continue

                # Collect author_user_id if different from user
                reply_author_id = (
                    str(reply.get("user_id")) if reply.get("user_id") else user_id
                )
                if (
                    reply_author_id
                    and reply_author_id not in seen_users
                    and is_valid_numeric_id(reply_author_id)
                ):
                    referenced_user_ids.add(reply_author_id)

                # Collect referenced user IDs for placeholder records
                reply_to_uid = (
                    str(reply.get("reply_to_user_id"))
                    if reply.get("reply_to_user_id")
                    else None
                )
                if (
                    reply_to_uid
                    and reply_to_uid not in seen_users
                    and is_valid_numeric_id(reply_to_uid)
                ):
                    referenced_user_ids.add(reply_to_uid)

                interactions_to_insert.append(
                    (
                        int(post_id),
                        "reply",
                        None,  # community_id is always NULL for seed graph
                        int(reply.get("user_id", user_id)),
                        reply.get("text"),
                        parse_twitter_date(reply.get("created_at")),
                        int(reply.get("reply_to_post_id"))
                        if reply.get("reply_to_post_id")
                        else None,
                        int(reply.get("reply_to_user_id"))
                        if reply.get("reply_to_user_id")
                        else None,
                        None,
                        None,
                        None,
                        None,
                    )
                )

        # Add placeholder records for referenced users not in seen_users
        for ref_user_id in referenced_user_ids:
            users_to_insert.append(
                (
                    int(ref_user_id),
                    "",  # empty username
                    None,
                )
            )

        # Deduplicate users by user_id
        users_dict = {}
        for user_tuple in users_to_insert:
            uid = user_tuple[0]
            if uid not in users_dict:
                users_dict[uid] = user_tuple
        users_to_insert = list(users_dict.values())

        # Deduplicate interactions by post_id (since community_id is always NULL)
        interactions_dict = {}
        for interaction_tuple in interactions_to_insert:
            pid = interaction_tuple[0]  # post_id
            if pid not in interactions_dict:
                interactions_dict[pid] = interaction_tuple
        interactions_to_insert = list(interactions_dict.values())

        # Insert users
        if users_to_insert:
            print(f"  💾 Inserting {len(users_to_insert)} users...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank.users (user_id, username, display_name)
                VALUES %s
                ON CONFLICT(user_id) DO UPDATE SET
                    username = CASE
                        WHEN EXCLUDED.username != '' THEN EXCLUDED.username
                        ELSE xrank.users.username
                    END,
                    display_name = COALESCE(EXCLUDED.display_name, xrank.users.display_name),
                    updated_at = CURRENT_TIMESTAMP
                """,
                users_to_insert,
                page_size=1000,
            )

        # Insert interactions (handling NULL community_id specially)
        inserted_count = 0
        if interactions_to_insert:
            print(f"  💾 Inserting up to {len(interactions_to_insert)} interactions...")
            inserted_count = insert_interactions_with_null_community(
                cursor, interactions_to_insert
            )

        conn.commit()
        print(
            f"  ✅ Imported {len(users_to_insert)} users and {inserted_count} interactions"
        )
        db_skipped = len(interactions_to_insert) - inserted_count
        if skipped_count > 0 or db_skipped > 0:
            print(f"  ⏭️  Skipped {skipped_count + db_skipped} existing posts")

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error importing seed interactions: {e}")
        raise
    finally:
        cursor.close()

    return (
        len(users_to_insert),
        inserted_count,
        skipped_count + (len(interactions_to_insert) - inserted_count),
    )


def main():
    """Main function to import all seed interactions to database."""
    parser = argparse.ArgumentParser(
        description="Import seed interactions from raw/seed/ to database"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without inserting",
    )
    args = parser.parse_args()

    # Get seed user IDs from config.toml
    seed_user_ids = get_seed_user_ids_from_config()
    if not seed_user_ids:
        print("❌ No seed user IDs found in config.toml [seed_graph] section")
        sys.exit(1)

    print(f"📋 Configured seed user IDs: {', '.join(sorted(seed_user_ids))}")

    # Get script directory and project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, "..")
    raw_seed_dir = os.path.join(project_root, "raw", "seed")

    # Find all seed interactions files
    pattern = os.path.join(raw_seed_dir, "*_seed_interactions.json")
    all_interaction_files = glob.glob(pattern)

    # Filter to only include files for users in config.toml
    interaction_files = []
    for file_path in all_interaction_files:
        # Extract user_id from filename (format: userid_seed_interactions.json)
        filename = os.path.basename(file_path)
        user_id = filename.replace("_seed_interactions.json", "")
        if user_id in seed_user_ids:
            interaction_files.append(file_path)

    if not interaction_files:
        print(
            f"❌ No seed interactions files found for configured users in {raw_seed_dir}"
        )
        print(f"   Looking for user IDs: {', '.join(sorted(seed_user_ids))}")
        print("Please run fetch_interactions.py first.")
        sys.exit(1)

    print(f"🔗 SEED GRAPH INTERACTIONS IMPORTER")
    print(f"=" * 50)
    print(f"📁 Source directory: {raw_seed_dir}")
    print(f"📄 Found {len(all_interaction_files)} total interaction file(s)")
    print(f"📄 Processing {len(interaction_files)} file(s) for configured users")

    if args.dry_run:
        print(f"🔍 DRY RUN MODE - No data will be inserted")

    try:
        conn = get_db_connection()
        print(f"✅ Connected to database")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)

    try:
        # Get existing post IDs once to avoid repeated queries
        print(f"\n📊 Fetching existing post IDs...")
        existing_post_ids = get_existing_post_ids(conn)
        print(
            f"  Found {len(existing_post_ids)} existing posts with community_id = NULL"
        )

        total_users = 0
        total_interactions = 0
        total_skipped = 0

        for file_path in sorted(interaction_files):
            print(f"\n{'─' * 50}")
            users, interactions, skipped = process_seed_interactions_file(
                conn, Path(file_path), existing_post_ids, args.dry_run
            )
            total_users += users
            total_interactions += interactions
            total_skipped += skipped

            # Add newly inserted post IDs to the set to avoid duplicates across files
            if not args.dry_run:
                # Re-fetch to get the actual inserted IDs
                pass  # The deduplication in the file handles this

        print(f"\n{'=' * 50}")
        print(f"🎉 IMPORT COMPLETE")
        print(f"{'=' * 50}")
        print(f"📊 Total users imported: {total_users}")
        print(f"📊 Total interactions imported: {total_interactions}")
        print(f"📊 Total posts skipped (already exist): {total_skipped}")

    except Exception as e:
        print(f"\n❌ Error during import: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

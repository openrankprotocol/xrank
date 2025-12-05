#!/usr/bin/env python3
"""
Import raw JSON data from raw/ directory into PostgreSQL database.

Imports data from the following JSON files:
- {community_id}_members.json -> users, community_members tables
- {community_id}_following_network.json -> users, followings tables
- {community_id}_comment_graph.json -> interactions table
- {community_id}_members_interactions.json -> interactions table

Imports data from CSV files:
- scores/{community_id}.csv -> runs, scores tables
- seed/{community_id}.csv -> seed table

Usage:
    python3 import_data.py                           # Import all communities
    python3 import_data.py --community 123456        # Import specific community
    python3 import_data.py --dry-run                 # Show what would be imported without inserting

Requirements:
    - psycopg2 (install with: pip install psycopg2-binary)
    - Environment variable: DATABASE_URL (e.g., postgresql://user:pass@localhost:5432/dbname)

Database schema:
    See schemas/communities.sql, schemas/users.sql, schemas/followings.sql, schemas/interactions.sql
"""

import argparse
import csv
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


def process_members_file(conn, file_path: Path, dry_run: bool = False):
    """Process a members JSON file."""
    print(f"  📂 Loading members from: {file_path.name}")

    with open(file_path, "r") as f:
        data = json.load(f)

    community_id = data.get("community_id")
    members = data.get("members", [])

    if not community_id:
        print(f"  ⚠️  No community_id found in {file_path.name}")
        return 0, 0

    print(f"  📊 Found {len(members)} members")

    if dry_run:
        print(f"  🔍 Dry run - no data inserted")
        return len(members), len(members)

    cursor = conn.cursor()

    try:
        # Extract community metadata
        community_name = data.get("community_name")
        community_description = data.get("community_description")
        community_created_at = data.get("community_created_at")
        creator = data.get("creator", {})
        creator_id = (
            int(creator.get("user_id")) if creator and creator.get("user_id") else None
        )

        # Insert/update community with metadata
        cursor.execute(
            """
            INSERT INTO xrank.communities (community_id, name, description, created_at, creator_id, updated_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT(community_id) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, xrank.communities.name),
                description = COALESCE(EXCLUDED.description, xrank.communities.description),
                created_at = COALESCE(EXCLUDED.created_at, xrank.communities.created_at),
                creator_id = COALESCE(EXCLUDED.creator_id, xrank.communities.creator_id),
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(community_id),
                community_name,
                community_description,
                community_created_at,
                creator_id,
            ),
        )

        if community_name:
            print(f"  📊 Community: {community_name}")

        # Get moderators list and create a set of moderator user_ids
        moderators = data.get("moderators", [])
        moderator_ids = {str(m.get("user_id")) for m in moderators if m.get("user_id")}
        print(f"  📊 Found {len(moderator_ids)} moderators")

        # Prepare users and memberships
        users_data = []
        memberships_data = []
        seen_users = set()

        # Process regular members
        for member in members:
            user_id = member.get("user_id")
            if not user_id:
                continue

            user_id_str = str(user_id)
            if user_id_str in seen_users:
                continue
            seen_users.add(user_id_str)

            users_data.append(
                (
                    int(user_id),
                    member.get("username", ""),
                    member.get("display_name"),
                )
            )

            # Check if this member is a moderator
            role = "moderator" if user_id_str in moderator_ids else "member"

            memberships_data.append((int(community_id), int(user_id), role))

        # Process moderators (in case they're not in the members list)
        for moderator in moderators:
            user_id = moderator.get("user_id")
            if not user_id:
                continue

            user_id_str = str(user_id)
            if user_id_str in seen_users:
                continue
            seen_users.add(user_id_str)

            users_data.append(
                (
                    int(user_id),
                    moderator.get("username", ""),
                    moderator.get("display_name"),
                )
            )

            memberships_data.append((int(community_id), int(user_id), "moderator"))

        # Insert users
        if users_data:
            print(f"  💾 Inserting users...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank.users (user_id, username, display_name)
                VALUES %s
                ON CONFLICT(user_id) DO UPDATE SET
                    username = COALESCE(EXCLUDED.username, xrank.users.username),
                    display_name = COALESCE(EXCLUDED.display_name, xrank.users.display_name),
                    updated_at = CURRENT_TIMESTAMP
                """,
                users_data,
                page_size=1000,
            )

        # Insert memberships
        if memberships_data:
            print(f"  💾 Inserting memberships...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank.community_members (community_id, user_id, role)
                VALUES %s
                ON CONFLICT(community_id, user_id) DO UPDATE SET
                    role = EXCLUDED.role
                """,
                memberships_data,
                page_size=1000,
            )

        conn.commit()
        print(f"  ✅ Imported {len(members)} members for community {community_id}")

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error importing members: {e}")
        raise
    finally:
        cursor.close()

    return len(users_data), len(memberships_data)


def process_following_network_file(conn, file_path: Path, dry_run: bool = False):
    """Process a following network JSON file."""
    print(f"  📂 Loading following network from: {file_path.name}")

    with open(file_path, "r") as f:
        data = json.load(f)

    community_id = data.get("community_id")
    following_network = data.get("following_network", [])

    if not community_id:
        print(f"  ⚠️  No community_id found in {file_path.name}")
        return 0, 0

    # Count total followings
    total_followings = sum(len(u.get("following", [])) for u in following_network)
    print(
        f"  📊 Found {len(following_network)} users with {total_followings} following relationships"
    )

    if dry_run:
        print(f"  🔍 Dry run - no data inserted")
        return len(following_network), total_followings

    cursor = conn.cursor()

    try:
        # Ensure community exists
        cursor.execute(
            """
            INSERT INTO xrank.communities (community_id, updated_at)
            VALUES (%s, CURRENT_TIMESTAMP)
            ON CONFLICT(community_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
            """,
            (int(community_id),),
        )

        # Prepare users and followings
        users_data = []
        followings_data = []

        for user_entry in following_network:
            user_id = user_entry.get("user_id")
            if not user_id:
                continue

            users_data.append(
                (
                    int(user_id),
                    user_entry.get("username", ""),
                    user_entry.get("display_name"),
                )
            )

            for following_id in user_entry.get("following", []):
                if following_id:
                    followings_data.append(
                        (int(user_id), int(following_id), int(community_id))
                    )

        # Insert users
        if users_data:
            print(f"  💾 Inserting users...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank.users (user_id, username, display_name)
                VALUES %s
                ON CONFLICT(user_id) DO UPDATE SET
                    username = COALESCE(EXCLUDED.username, xrank.users.username),
                    display_name = COALESCE(EXCLUDED.display_name, xrank.users.display_name),
                    updated_at = CURRENT_TIMESTAMP
                """,
                users_data,
                page_size=1000,
            )

        # Insert followings
        if followings_data:
            print(f"  💾 Inserting followings...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank.followings (follower_user_id, following_user_id, community_id)
                VALUES %s
                ON CONFLICT (follower_user_id, following_user_id, community_id) DO NOTHING
                """,
                followings_data,
                page_size=1000,
            )

        conn.commit()
        print(
            f"  ✅ Imported {len(users_data)} users and {len(followings_data)} following relationships"
        )

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error importing following network: {e}")
        raise
    finally:
        cursor.close()

    return len(users_data), len(followings_data)


def process_comment_graph_file(conn, file_path: Path, dry_run: bool = False):
    """Process a comment graph JSON file."""
    print(f"  📂 Loading comment graph from: {file_path.name}")

    with open(file_path, "r") as f:
        data = json.load(f)

    community_id = data.get("community_id")
    comment_graph = data.get("comment_graph", [])

    if not community_id:
        print(f"  ⚠️  No community_id found in {file_path.name}")
        return 0, 0

    print(f"  📊 Found {len(comment_graph)} comments/posts")

    if dry_run:
        print(f"  🔍 Dry run - no data inserted")
        return 0, len(comment_graph)

    cursor = conn.cursor()

    try:
        # Ensure community exists
        cursor.execute(
            """
            INSERT INTO xrank.communities (community_id, updated_at)
            VALUES (%s, CURRENT_TIMESTAMP)
            ON CONFLICT(community_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
            """,
            (int(community_id),),
        )

        # Prepare users and interactions
        users_data = []
        interactions_data = []
        seen_users = set()
        # Collect all referenced user IDs that need placeholder records
        referenced_user_ids = set()

        for comment in comment_graph:
            commenter_user_id = comment.get("commenter_user_id")
            comment_id = comment.get("comment_id")

            if not commenter_user_id or not comment_id:
                continue

            # Collect user if we have username info
            if (
                comment.get("commenter_username")
                and commenter_user_id not in seen_users
            ):
                users_data.append(
                    (
                        int(commenter_user_id),
                        comment.get("commenter_username", ""),
                        comment.get("commenter_display_name"),
                    )
                )
                seen_users.add(commenter_user_id)

            # Collect original post author
            original_author_id = comment.get("original_post_author_id")
            if (
                original_author_id
                and comment.get("original_post_author_username")
                and original_author_id not in seen_users
            ):
                users_data.append(
                    (
                        int(original_author_id),
                        comment.get("original_post_author_username", ""),
                        None,  # display_name
                    )
                )
                seen_users.add(original_author_id)

            # Collect referenced user IDs for placeholder records
            in_reply_to_user_id = comment.get("in_reply_to_user_id")
            if in_reply_to_user_id and in_reply_to_user_id not in seen_users:
                referenced_user_ids.add(in_reply_to_user_id)

            # Determine interaction type
            in_reply_to_status_id = comment.get("in_reply_to_status_id")
            interaction_type = "reply" if in_reply_to_status_id else "post"

            interactions_data.append(
                (
                    int(comment_id),
                    interaction_type,
                    int(community_id),
                    int(commenter_user_id),
                    comment.get("comment_text"),
                    parse_twitter_date(comment.get("comment_created_at")),
                    int(in_reply_to_status_id) if in_reply_to_status_id else None,
                    int(in_reply_to_user_id) if in_reply_to_user_id else None,
                    None,  # retweeted_post_id
                    None,  # retweeted_user_id
                    None,  # quoted_post_id
                    None,  # quoted_user_id
                )
            )

        # Add placeholder records for referenced users not in seen_users
        for ref_user_id in referenced_user_ids:
            users_data.append(
                (
                    int(ref_user_id),
                    "",  # empty username
                    None,
                )
            )

        # Deduplicate users_data by user_id (first element of tuple)
        users_dict = {}
        for user_tuple in users_data:
            user_id = user_tuple[0]
            if user_id not in users_dict:
                users_dict[user_id] = user_tuple
        users_data = list(users_dict.values())

        # Insert users
        if users_data:
            print(f"  💾 Inserting users...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank.users (user_id, username, display_name)
                VALUES %s
                ON CONFLICT(user_id) DO UPDATE SET
                    username = COALESCE(EXCLUDED.username, xrank.users.username),
                    display_name = COALESCE(EXCLUDED.display_name, xrank.users.display_name),
                    updated_at = CURRENT_TIMESTAMP
                """,
                users_data,
                page_size=1000,
            )

        # Insert interactions
        if interactions_data:
            print(f"  💾 Inserting interactions...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank.interactions (post_id, interaction_type, community_id, author_user_id,
                    text, created_at, reply_to_post_id, reply_to_user_id,
                    retweeted_post_id, retweeted_user_id, quoted_post_id, quoted_user_id)
                VALUES %s
                ON CONFLICT(post_id, community_id) DO UPDATE SET
                    text = COALESCE(EXCLUDED.text, xrank.interactions.text),
                    created_at = COALESCE(EXCLUDED.created_at, xrank.interactions.created_at)
                """,
                interactions_data,
                page_size=1000,
            )

        conn.commit()
        print(
            f"  ✅ Imported {len(users_data)} users and {len(interactions_data)} interactions"
        )

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error importing comment graph: {e}")
        raise
    finally:
        cursor.close()

    return len(users_data), len(interactions_data)


def process_members_interactions_file(conn, file_path: Path, dry_run: bool = False):
    """Process a members interactions JSON file."""
    print(f"  📂 Loading members interactions from: {file_path.name}")

    with open(file_path, "r") as f:
        data = json.load(f)

    community_id = data.get("community_id")
    members_interactions = data.get("members_interactions", [])

    if not community_id:
        print(f"  ⚠️  No community_id found in {file_path.name}")
        return 0, 0

    # Count total interactions
    total_posts = sum(len(m.get("posts", [])) for m in members_interactions)
    total_replies = sum(len(m.get("replies", [])) for m in members_interactions)
    print(
        f"  📊 Found {len(members_interactions)} members with {total_posts} posts and {total_replies} replies"
    )

    if dry_run:
        print(f"  🔍 Dry run - no data inserted")
        return len(members_interactions), total_posts + total_replies

    cursor = conn.cursor()

    try:
        # Ensure community exists
        cursor.execute(
            """
            INSERT INTO xrank.communities (community_id, updated_at)
            VALUES (%s, CURRENT_TIMESTAMP)
            ON CONFLICT(community_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
            """,
            (int(community_id),),
        )

        # Prepare users and interactions
        users_data = []
        interactions_data = []
        seen_users = set()  # Store as strings for consistent comparison
        # Collect all referenced user IDs that need placeholder records
        referenced_user_ids = set()  # Store as strings for consistent comparison

        for member in members_interactions:
            user_id = str(member.get("user_id")) if member.get("user_id") else None
            if not user_id:
                continue

            if user_id not in seen_users:
                users_data.append(
                    (
                        int(user_id),
                        member.get("username", ""),
                        member.get("display_name"),
                    )
                )
                seen_users.add(user_id)

            # Process posts
            for post in member.get("posts", []):
                post_id = post.get("post_id")
                if not post_id:
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

                # Collect author_user_id if different from member
                post_author_id = (
                    str(post.get("user_id")) if post.get("user_id") else user_id
                )
                if post_author_id and post_author_id not in seen_users:
                    referenced_user_ids.add(post_author_id)

                # Collect referenced user IDs for placeholder records
                reply_to_uid = (
                    str(post.get("reply_to_user_id"))
                    if post.get("reply_to_user_id")
                    else None
                )
                if reply_to_uid and reply_to_uid not in seen_users:
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
                ):
                    referenced_user_ids.add(original_creator_id)
                quoted_uid = str(quoted_user_id) if quoted_user_id else None
                if quoted_uid and quoted_uid not in seen_users:
                    referenced_user_ids.add(quoted_uid)

                interactions_data.append(
                    (
                        int(post_id),
                        interaction_type,
                        int(community_id),  # Always use the file's community_id
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
            for reply in member.get("replies", []):
                post_id = reply.get("post_id")
                if not post_id:
                    continue

                # Collect author_user_id if different from member
                reply_author_id = (
                    str(reply.get("user_id")) if reply.get("user_id") else user_id
                )
                if reply_author_id and reply_author_id not in seen_users:
                    referenced_user_ids.add(reply_author_id)

                # Collect referenced user IDs for placeholder records
                reply_to_uid = (
                    str(reply.get("reply_to_user_id"))
                    if reply.get("reply_to_user_id")
                    else None
                )
                if reply_to_uid and reply_to_uid not in seen_users:
                    referenced_user_ids.add(reply_to_uid)

                interactions_data.append(
                    (
                        int(post_id),
                        "reply",
                        int(community_id),  # Always use the file's community_id
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
            users_data.append(
                (
                    int(ref_user_id),
                    "",  # empty username
                    None,
                )
            )

        # Deduplicate users_data by user_id (first element of tuple)
        users_dict = {}
        for user_tuple in users_data:
            user_id = user_tuple[0]
            if user_id not in users_dict:
                users_dict[user_id] = user_tuple
        users_data = list(users_dict.values())

        # Deduplicate interactions_data by (post_id, community_id) - indices 0 and 2
        interactions_dict = {}
        for interaction_tuple in interactions_data:
            key = (
                interaction_tuple[0],
                interaction_tuple[2],
            )  # (post_id, community_id)
            if key not in interactions_dict:
                interactions_dict[key] = interaction_tuple
        interactions_data = list(interactions_dict.values())

        # Insert users
        if users_data:
            print(f"  💾 Inserting users...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank.users (user_id, username, display_name)
                VALUES %s
                ON CONFLICT(user_id) DO UPDATE SET
                    username = COALESCE(EXCLUDED.username, xrank.users.username),
                    display_name = COALESCE(EXCLUDED.display_name, xrank.users.display_name),
                    updated_at = CURRENT_TIMESTAMP
                """,
                users_data,
                page_size=1000,
            )

        # Insert interactions
        if interactions_data:
            print(f"  💾 Inserting interactions...")
            execute_values(
                cursor,
                """
                INSERT INTO xrank.interactions (post_id, interaction_type, community_id, author_user_id,
                    text, created_at, reply_to_post_id, reply_to_user_id,
                    retweeted_post_id, retweeted_user_id, quoted_post_id, quoted_user_id)
                VALUES %s
                ON CONFLICT(post_id, community_id) DO UPDATE SET
                    text = COALESCE(EXCLUDED.text, xrank.interactions.text),
                    created_at = COALESCE(EXCLUDED.created_at, xrank.interactions.created_at)
                """,
                interactions_data,
                page_size=1000,
            )

        conn.commit()
        print(
            f"  ✅ Imported {len(users_data)} users and {len(interactions_data)} interactions"
        )

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error importing members interactions: {e}")
        raise
    finally:
        cursor.close()

    return len(users_data), len(interactions_data)


def process_scores_file(
    conn, file_path: Path, community_id: int, run_id: int, dry_run: bool = False
):
    """Process a scores CSV file and import into scores table."""
    print(f"  📂 Loading scores from: {file_path.name}")

    scores_data = []
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row.get("i")
            score = row.get("v")
            if user_id and score:
                scores_data.append((community_id, run_id, int(user_id), float(score)))

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
                INSERT INTO xrank.scores (community_id, run_id, user_id, score)
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
    conn, file_path: Path, community_id: int, run_id: int, dry_run: bool = False
):
    """Process a seed CSV file and import into seed table."""
    print(f"  📂 Loading seed from: {file_path.name}")

    seed_data = []
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row.get("i")
            score = row.get("v")
            if user_id and score:
                seed_data.append((community_id, run_id, int(user_id), float(score)))

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
                INSERT INTO xrank.seeds (community_id, run_id, user_id, score)
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
        print(f"  ❌ Error importing seed: {e}")
        raise
    finally:
        cursor.close()

    return len(seed_data)


def create_run(conn, community_id: str, days_back: int, dry_run: bool = False):
    """Create a new run entry and return the run_id (per-community incrementing)."""
    if dry_run:
        print(f"  🔍 Dry run - would create run for community {community_id}")
        return None

    cursor = conn.cursor()

    try:
        # Get the next run_id for this community
        cursor.execute(
            """
            SELECT COALESCE(MAX(run_id), 0) + 1
            FROM xrank.runs
            WHERE community_id = %s
            """,
            (int(community_id),),
        )
        run_id = cursor.fetchone()[0]

        # Insert the new run
        cursor.execute(
            """
            INSERT INTO xrank.runs (community_id, run_id, days_back)
            VALUES (%s, %s, %s)
            """,
            (int(community_id), run_id, days_back),
        )
        conn.commit()
        print(f"  ✅ Created run {run_id} for community {community_id}")
        return run_id

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error creating run: {e}")
        raise
    finally:
        cursor.close()


def import_community(
    conn, community_id: str, files: dict, days_back: int, dry_run: bool = False
):
    """
    Import all data for a single community.

    Args:
        conn: Database connection
        community_id: Community ID
        files: Dict of file_type -> file_path
        days_back: Number of days of data used (from config)
        dry_run: If True, don't actually insert data

    Returns:
        dict: Counts of imported items
    """
    counts = {
        "users": 0,
        "memberships": 0,
        "followings": 0,
        "interactions": 0,
        "scores": 0,
        "seed": 0,
    }

    # Process in order: members first (to establish users), then others
    if "members" in files:
        users, memberships = process_members_file(conn, files["members"], dry_run)
        counts["users"] += users
        counts["memberships"] += memberships
        print()

    if "following_network" in files:
        users, followings = process_following_network_file(
            conn, files["following_network"], dry_run
        )
        counts["users"] += users
        counts["followings"] += followings
        print()

    if "comment_graph" in files:
        users, interactions = process_comment_graph_file(
            conn, files["comment_graph"], dry_run
        )
        counts["users"] += users
        counts["interactions"] += interactions
        print()

    if "members_interactions" in files:
        users, interactions = process_members_interactions_file(
            conn, files["members_interactions"], dry_run
        )
        counts["users"] += users
        counts["interactions"] += interactions
        print()

    # Import scores and seed if available
    if "scores" in files or "seed" in files:
        print("📊 Importing scores and seed data...")

        # Ensure community exists before creating run (foreign key constraint)
        if not dry_run:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    INSERT INTO xrank.communities (community_id, updated_at)
                    VALUES (%s, CURRENT_TIMESTAMP)
                    ON CONFLICT(community_id) DO UPDATE SET
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (int(community_id),),
                )
                conn.commit()
            finally:
                cursor.close()

        run_id = create_run(conn, community_id, days_back, dry_run)

        if "scores" in files and run_id:
            scores_count = process_scores_file(
                conn, files["scores"], int(community_id), run_id, dry_run
            )
            counts["scores"] += scores_count
            print()

        if "seed" in files and run_id:
            seed_count = process_seed_file(
                conn, files["seed"], int(community_id), run_id, dry_run
            )
            counts["seed"] += seed_count
            print()

    return counts


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Import raw JSON data into PostgreSQL database"
    )
    parser.add_argument(
        "--community",
        type=str,
        help="Specific community ID to import (otherwise imports all)",
    )
    parser.add_argument(
        "--raw",
        type=str,
        default="raw",
        help="Path to raw data directory (default: raw)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without actually inserting",
    )

    args = parser.parse_args()

    print("📥 Import X/Twitter Data to PostgreSQL\n")

    # Load config for days_back
    script_dir = Path(__file__).parent
    config_path = script_dir / "config.toml"
    days_back = 365  # default
    if config_path.exists():
        try:
            config = toml.load(config_path)
            days_back = config.get("data", {}).get("days_back", 365)
            print(f"📅 Using days_back={days_back} from config.toml")
        except Exception as e:
            print(f"⚠️  Could not load config.toml: {e}, using default days_back=365")
    else:
        print(f"⚠️  config.toml not found, using default days_back=365")

    # Resolve paths relative to script directory
    raw_dir = script_dir / args.raw
    scores_dir = script_dir / "scores"
    seed_dir = script_dir / "seed"

    if not raw_dir.exists():
        print(f"❌ Error: Raw data directory not found: {raw_dir}")
        sys.exit(1)

    # Find all JSON files in raw directory
    json_files = sorted(raw_dir.glob("*.json"))

    if not json_files:
        print(f"❌ Error: No JSON files found in {raw_dir}")
        sys.exit(1)

    # Group files by community
    communities = {}
    for file_path in json_files:
        parts = file_path.stem.split("_", 1)
        if len(parts) < 2:
            continue

        comm_id = parts[0]
        file_type = parts[1]

        if args.community and comm_id != args.community:
            continue

        if comm_id not in communities:
            communities[comm_id] = {}
        communities[comm_id][file_type] = file_path

    # Add scores files
    if scores_dir.exists():
        for file_path in sorted(scores_dir.glob("*.csv")):
            comm_id = file_path.stem
            if args.community and comm_id != args.community:
                continue
            if comm_id not in communities:
                communities[comm_id] = {}
            communities[comm_id]["scores"] = file_path

    # Add seed files
    if seed_dir.exists():
        for file_path in sorted(seed_dir.glob("*.csv")):
            comm_id = file_path.stem
            if args.community and comm_id != args.community:
                continue
            if comm_id == "seed_graph":
                continue  # Skip seed_graph.csv
            if comm_id not in communities:
                communities[comm_id] = {}
            communities[comm_id]["seed"] = file_path

    if not communities:
        if args.community:
            print(f"❌ Error: No files found for community {args.community}")
        else:
            print(f"❌ Error: No community data files found")
        sys.exit(1)

    print(f"Found {len(communities)} community(ies) to process")

    if args.dry_run:
        print("Mode: Dry run (no data will be inserted)\n")
    else:
        print()

    # Connect to database
    try:
        conn = get_db_connection()
        print("✅ Connected to database\n")
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)

    total_counts = {
        "users": 0,
        "memberships": 0,
        "followings": 0,
        "interactions": 0,
        "scores": 0,
        "seed": 0,
    }

    try:
        for comm_id, files in communities.items():
            print(f"{'=' * 60}")
            print(f"Community: {comm_id}")
            print(f"{'=' * 60}")

            counts = import_community(
                conn, comm_id, files, days_back=days_back, dry_run=args.dry_run
            )

            for key in total_counts:
                total_counts[key] += counts[key]

    finally:
        conn.close()

    print(f"{'=' * 60}")
    print("📊 Summary")
    print(f"{'=' * 60}")
    print(f"   Total communities: {len(communities)}")
    print(f"   Total users: {total_counts['users']}")
    print(f"   Total memberships: {total_counts['memberships']}")
    print(f"   Total followings: {total_counts['followings']}")
    print(f"   Total interactions: {total_counts['interactions']}")
    print(f"   Total scores: {total_counts['scores']}")
    print(f"   Total seed users: {total_counts['seed']}")
    if args.dry_run:
        print("   (Dry run - no data was inserted)")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

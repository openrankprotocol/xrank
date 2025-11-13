#!/usr/bin/env python3
"""
Comment Graph Fetcher

This script analyzes comment interactions within a Twitter/X community by:
1. Loading raw/[community_id]_members.json to create a master list
2. Fetching all community posts using /community-tweets (sorted by Recency)
3. Getting comments for each post using /comments-v2 (sorted by Relevance) in parallel (20 posts at a time)
4. Building a comment graph of interactions between users in the master list
5. Saving to raw/[community_id]_comment_graph.json

Uses endpoints:
- /twitter/community/tweets for community posts
- /comments-v2 (still using old API - needs new endpoint when available)

Rate limited to 1,000 requests per second to comply with API limits.
"""

import http.client
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import toml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class RateLimiter:
    """Rate limiter to ensure we don't exceed API rate limits"""

    def __init__(self, requests_per_second=1000):
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second  # Minimum time between requests
        self.last_request_time = 0
        self.lock = threading.Lock()

    def wait_for_token(self):
        """Wait until enough time has passed since last request"""
        with self.lock:
            now = time.time()
            time_since_last = now - self.last_request_time

            if time_since_last < self.min_interval:
                wait_time = self.min_interval - time_since_last
                time.sleep(wait_time)

            self.last_request_time = time.time()


# Initialize global rate limiter and request counter
rate_limiter = None
request_count = 0
start_time = None


def load_config():
    """Load configuration from config.toml"""
    try:
        with open("config.toml", "r") as f:
            return toml.load(f)
    except FileNotFoundError:
        print("config.toml not found, using default values")
        return {
            "data": {"days_back": 730, "post_limit": 10000},
            "communities": {"ids": ["1601841656147345410"]},
            "output": {"raw_data_dir": "./raw"},
            "rate_limiting": {
                "requests_per_second": 10,
                "request_delay": 0.1,
                "community_delay": 2.0,
            },
        }
    except Exception as e:
        print(f"Error loading config: {e}")
        return None


def get_api_key(use_old_api=False):
    """Get API key from environment with proper formatting

    Args:
        use_old_api: If True, returns RAPIDAPI_KEY for old API, else TWITTER_API_KEY for new API
    """
    if use_old_api:
        api_key = os.getenv("RAPIDAPI_KEY")
        key_name = "RAPIDAPI_KEY"
    else:
        api_key = os.getenv("TWITTER_API_KEY")
        key_name = "TWITTER_API_KEY"

    if not api_key:
        raise ValueError(f"{key_name} not found in environment variables")

    # Remove surrounding quotes if present
    if api_key.startswith('"') and api_key.endswith('"'):
        api_key = api_key[1:-1]
    elif api_key.startswith("'") and api_key.endswith("'"):
        api_key = api_key[1:-1]

    if not api_key.strip():
        raise ValueError(f"{key_name} is empty after cleaning")

    return api_key


def make_request(endpoint, params=None, max_retries=3, use_old_api=False):
    """Make HTTP request with rate limiting and exponential backoff

    Supports both new twitterapi.io API and old RapidAPI for backwards compatibility.
    Old API is used for endpoints that haven't been migrated yet (e.g., comments-v2).
    """
    global request_count, start_time

    # Initialize start time on first request
    if start_time is None:
        start_time = time.time()

    for attempt in range(max_retries):
        # Wait for rate limiter before making request
        if rate_limiter:
            rate_limiter.wait_for_token()

        # Increment request counter
        request_count += 1

        # Log progress every 100 requests
        if request_count % 100 == 0:
            elapsed = time.time() - start_time
            rate = request_count / elapsed if elapsed > 0 else 0
            print(f"📊 API: {request_count} requests, {rate:.2f}/sec")

        try:
            # Determine which API to use based on endpoint
            # Comments endpoint still uses old RapidAPI
            if use_old_api or endpoint.startswith("/comments"):
                conn = http.client.HTTPSConnection("twitter241.p.rapidapi.com")
                headers = {
                    "x-rapidapi-key": get_api_key(use_old_api=True),
                    "x-rapidapi-host": "twitter241.p.rapidapi.com",
                }
                # Old API uses string params
                if isinstance(params, dict):
                    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                    full_endpoint = (
                        f"{endpoint}?{query_string}" if query_string else endpoint
                    )
                else:
                    full_endpoint = f"{endpoint}?{params}" if params else endpoint
            else:
                # New API
                conn = http.client.HTTPSConnection("api.twitterapi.io")
                headers = {
                    "X-API-Key": get_api_key(use_old_api=False),
                }
                # Build URL with query parameters
                if params:
                    if isinstance(params, dict):
                        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                    else:
                        query_string = params
                    full_endpoint = f"{endpoint}?{query_string}"
                else:
                    full_endpoint = endpoint

            conn.request("GET", full_endpoint, headers=headers)

            res = conn.getresponse()
            data = res.read()
            conn.close()

            if res.status == 200:
                return json.loads(data.decode("utf-8"))
            elif res.status == 429:  # Rate limit exceeded
                if attempt < max_retries - 1:
                    backoff_time = 2**attempt
                    print(f"⚠️  Rate limit, waiting {backoff_time}s...")
                    time.sleep(backoff_time)
                    continue
                else:
                    return None
            elif res.status >= 500:  # Server errors
                if attempt < max_retries - 1:
                    backoff_time = 2**attempt
                    print(f"⚠️  Server error {res.status}, retrying...")
                    time.sleep(backoff_time)
                    continue
                else:
                    return None
            else:
                print(f"❌ HTTP {res.status}")
                return None

        except json.JSONDecodeError as e:
            if attempt < max_retries - 1:
                backoff_time = 2**attempt
                time.sleep(backoff_time)
                continue
            else:
                return None
        except Exception as e:
            if attempt < max_retries - 1:
                backoff_time = 2**attempt
                time.sleep(backoff_time)
                continue
            else:
                return None

    return None


def load_master_list(community_id, raw_data_dir):
    """Load and combine all user lists to create master list"""
    master_users = {}  # Use dict to avoid duplicates

    # Load members
    members_file = os.path.join(raw_data_dir, f"{community_id}_members.json")
    if os.path.exists(members_file):
        try:
            with open(members_file, "r", encoding="utf-8") as f:
                members_data = json.load(f)

            # Add regular members
            members_count = 0
            for member in members_data.get("members", []):
                if member.get("username") and member.get("user_id"):
                    master_users[member["user_id"]] = {
                        "username": member["username"],
                        "user_id": member["user_id"],
                        "display_name": member.get("display_name", ""),
                        "type": "member",
                    }
                    members_count += 1

            # Add moderators
            moderators_count = 0
            for moderator in members_data.get("moderators", []):
                if moderator.get("username") and moderator.get("user_id"):
                    master_users[moderator["user_id"]] = {
                        "username": moderator["username"],
                        "user_id": moderator["user_id"],
                        "display_name": moderator.get("display_name", ""),
                        "type": "moderator",
                    }
                    moderators_count += 1

            print(f"✓ Loaded {members_count} members and {moderators_count} moderators")

        except Exception as e:
            print(f"❌ Error loading members: {e}")

    master_list = list(master_users.values())
    print(f"✓ Master list: {len(master_list)} users")

    # Create lookup dictionaries for fast searching
    user_id_lookup = {user["user_id"]: user for user in master_list}
    username_lookup = {
        user["username"].lower(): user for user in master_list if user.get("username")
    }

    return master_list, user_id_lookup, username_lookup


def is_post_within_days(created_at_str, days_back):
    """Check if post is within the specified days back"""
    try:
        if not created_at_str:
            return False

        from datetime import timezone

        # Try multiple date formats
        post_date = None

        # Format 1: Twitter's old format "Wed Nov 12 15:59:13 +0000 2025"
        try:
            post_date = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
        except:
            pass

        # Format 2: ISO 8601 with milliseconds "2024-01-15T10:30:45.123Z"
        if not post_date and "." in created_at_str:
            try:
                post_date = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                post_date = post_date.replace(tzinfo=timezone.utc)
            except:
                pass

        # Format 3: ISO 8601 without milliseconds "2024-01-15T10:30:45Z"
        if not post_date:
            try:
                post_date = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%SZ")
                post_date = post_date.replace(tzinfo=timezone.utc)
            except:
                pass

        if not post_date:
            return False

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)

        return post_date >= cutoff_date
    except Exception as e:
        return False


def get_community_posts(community_id, days_back):
    """Get all posts from a community within the specified time range using new API"""
    print(f"Fetching community posts for {community_id}")

    posts = []
    cursor = None
    page = 0
    max_pages = 50  # Reasonable limit

    while page < max_pages:
        page += 1

        params = {"community_id": community_id}
        if cursor:
            params["cursor"] = cursor

        response = make_request("/twitter/community/tweets", params)

        if not response:
            break

        # Check API response status
        if response.get("status") != "success":
            print(f"❌ API error: {response.get('msg', 'Unknown error')}")
            break

        found_posts = False

        try:
            # Extract tweets from new API format (inside data object)
            data = response.get("data", {})
            tweets = data.get("tweets", [])

            if not tweets:
                break

            for tweet in tweets:
                # Check if within date range
                created_at = tweet.get("createdAt")
                if is_post_within_days(created_at, days_back):
                    post_info = extract_post_info(tweet)
                    if post_info:
                        posts.append(post_info)
                        found_posts = True
                else:
                    # If we hit content outside date range, stop
                    print(f"  Reached posts outside date range")
                    return posts

            # Check for next page
            if response.get("has_next_page"):
                cursor = response.get("next_cursor")
            else:
                cursor = None

            if not cursor or not found_posts:
                break

            if page % 5 == 0:
                print(f"  Page {page}: {len(posts)} posts so far")

        except Exception as e:
            print(f"❌ Error on page {page}: {e}")
            import traceback

            traceback.print_exc()
            break

    print(f"✓ Found {len(posts)} community posts")
    return posts


def extract_post_info(tweet):
    """Extract relevant information from a post using new API format"""
    try:
        # New API has simpler structure
        author = tweet.get("author", {})

        return {
            "post_id": tweet.get("id", ""),
            "text": tweet.get("text", ""),
            "created_at": tweet.get("createdAt", ""),
            "user_id": author.get("id", ""),
            "username": author.get("userName", ""),
            "display_name": author.get("name", ""),
            "retweet_count": tweet.get("retweetCount", 0),
            "favorite_count": tweet.get("likeCount", 0),
            "reply_count": tweet.get("replyCount", 0),
        }

    except Exception as e:
        return None


def analyze_response_structure(response, indent=""):
    """Helper function to analyze and log API response structure for debugging"""
    try:
        if not response or not isinstance(response, dict):
            print(f"{indent}Invalid response: {type(response)}")
            return

        print(f"{indent}Response keys: {list(response.keys())}")

        if "result" in response:
            result = response["result"]
            print(f"{indent}Result keys: {list(result.keys())}")

            if "instructions" in result:
                instructions = result["instructions"]
                print(f"{indent}Instructions count: {len(instructions)}")

                for i, instruction in enumerate(
                    instructions[:2]
                ):  # Analyze first 2 instructions
                    print(f"{indent}Instruction {i}:")
                    print(f"{indent}  Type: {instruction.get('type')}")

                    if "entries" in instruction:
                        entries = instruction["entries"]
                        print(f"{indent}  Entries count: {len(entries)}")

                        for j, entry in enumerate(
                            entries[:3]
                        ):  # Analyze first 3 entries
                            entry_id = entry.get("entryId", "No ID")
                            print(f"{indent}    Entry {j}: {entry_id}")

                            if "content" in entry:
                                content = entry["content"]
                                print(
                                    f"{indent}      Content keys: {list(content.keys())}"
                                )

                                if "itemContent" in content:
                                    item_content = content["itemContent"]
                                    print(
                                        f"{indent}        ItemContent keys: {list(item_content.keys())}"
                                    )

                                    if "tweet_results" in item_content:
                                        tweet_results = item_content["tweet_results"]
                                        print(
                                            f"{indent}          TweetResults keys: {list(tweet_results.keys())}"
                                        )

                                        if "result" in tweet_results:
                                            tweet_data = tweet_results["result"]
                                            print(
                                                f"{indent}            Tweet data keys: {list(tweet_data.keys())}"
                                            )

                                            # Check for TweetWithVisibilityResults wrapper
                                            if "__typename" in tweet_data:
                                                print(
                                                    f"{indent}            __typename: {tweet_data['__typename']}"
                                                )

                                            if "tweet" in tweet_data:
                                                inner_tweet = tweet_data["tweet"]
                                                print(
                                                    f"{indent}            Inner tweet keys: {list(inner_tweet.keys())}"
                                                )

                                if "items" in content:  # For conversation threads
                                    items = content["items"]
                                    print(
                                        f"{indent}        Thread items count: {len(items)}"
                                    )

                            if j >= 2:  # Limit output
                                break

                    if i >= 1:  # Limit output
                        break

        if "cursor" in response:
            print(f"{indent}Top-level cursor keys: {list(response['cursor'].keys())}")

    except Exception as e:
        print(f"{indent}Error analyzing response structure: {e}")


def get_post_comments(post_id):
    """Get comments for a specific post with improved error handling and debug logging"""
    comments = []
    cursor = None
    page = 0
    max_pages = 10  # Reasonable limit per post

    while page < max_pages:
        page += 1

        params = f"pid={post_id}&rankingMode=Relevance&count=1000"
        if cursor:
            params += f"&cursor={cursor}"

        response = make_request("/comments-v2", params)

        if not response:
            break

        found_comments = False
        processed_entries = 0

        try:
            if "result" in response and "instructions" in response["result"]:
                instructions = response["result"]["instructions"]

                for instruction_idx, instruction in enumerate(instructions):
                    if (
                        instruction.get("type") == "TimelineAddEntries"
                        and "entries" in instruction
                    ):
                        entries = instruction["entries"]

                        for entry_idx, entry in enumerate(entries):
                            entry_id = entry.get("entryId", "")
                            processed_entries += 1

                            if "tweet-" in entry_id:
                                entry_content = entry.get("content", {})

                                if (
                                    "itemContent" in entry_content
                                    and "tweet_results" in entry_content["itemContent"]
                                ):
                                    tweet_results = entry_content["itemContent"][
                                        "tweet_results"
                                    ]
                                    comment_data = tweet_results.get("result", {})

                                    if comment_data:
                                        comment_info = extract_comment_info(
                                            comment_data, post_id
                                        )
                                        if comment_info:
                                            comments.append(comment_info)
                                            found_comments = True

                            elif "conversationthread-" in entry_id:
                                # Handle conversation threads that contain multiple comments
                                if "content" in entry and "items" in entry["content"]:
                                    thread_items = entry["content"]["items"]

                                    for item_idx, item in enumerate(thread_items):
                                        if (
                                            "item" in item
                                            and "itemContent" in item["item"]
                                        ):
                                            item_content = item["item"]["itemContent"]
                                            if "tweet_results" in item_content:
                                                tweet_results = item_content[
                                                    "tweet_results"
                                                ]
                                                comment_data = tweet_results.get(
                                                    "result", {}
                                                )

                                                if comment_data:
                                                    comment_info = extract_comment_info(
                                                        comment_data, post_id
                                                    )
                                                    if comment_info:
                                                        comments.append(comment_info)
                                                        found_comments = True

            # Look for cursor for next page
            cursor = None
            if "result" in response and "instructions" in response["result"]:
                instructions = response["result"]["instructions"]
                for instruction in instructions:
                    if instruction.get("type") == "TimelineAddEntries":
                        entries = instruction.get("entries", [])
                        for entry in entries:
                            if entry.get("entryId", "").startswith("cursor-bottom-"):
                                cursor_content = entry.get("content", {})
                                if "value" in cursor_content:
                                    cursor = cursor_content["value"]
                                    break

            # Also check for cursor at the top level
            if not cursor and "cursor" in response and "bottom" in response["cursor"]:
                cursor = response["cursor"]["bottom"]

            print(
                f"      Page {page} results: processed {processed_entries} entries, found {len([c for c in comments if c])} new comments"
            )

            if not cursor or not found_comments:
                break

        except Exception as e:
            break

    if comments:
        print(
            f"    Collected {len(comments)} total comments from {page} pages for post {post_id}"
        )

    return comments


def extract_comment_info(comment_data, original_post_id):
    """Extract relevant information from a comment - handles TweetWithVisibilityResults wrapper"""
    try:
        # Handle TweetWithVisibilityResults wrapper and multiple nesting patterns
        tweet_data = comment_data
        wrapper_found = False

        # Check for TweetWithVisibilityResults wrapper by typename
        if (
            "__typename" in comment_data
            and comment_data["__typename"] == "TweetWithVisibilityResults"
        ):
            if "tweet" in comment_data:
                tweet_data = comment_data["tweet"]
                wrapper_found = True

        # Check for tweet field containing the actual tweet data
        elif "tweet" in comment_data and isinstance(comment_data["tweet"], dict):
            if "legacy" in comment_data["tweet"]:
                tweet_data = comment_data["tweet"]
                wrapper_found = True

        # Check for result wrapper (sometimes tweets are wrapped in result)
        elif "result" in comment_data and isinstance(comment_data["result"], dict):
            result_data = comment_data["result"]
            if "__typename" in result_data and result_data["__typename"] == "Tweet":
                tweet_data = result_data
            elif "legacy" in result_data:
                tweet_data = result_data

        # Extract legacy data (main tweet content) with multiple fallbacks
        legacy = None
        if "legacy" in tweet_data:
            legacy = tweet_data["legacy"]
        elif "tweet" in tweet_data and "legacy" in tweet_data["tweet"]:
            # Sometimes there's another level of nesting
            legacy = tweet_data["tweet"]["legacy"]
        else:
            return None

        # Extract core data (user information)
        core = tweet_data.get("core", {})

        # Extract user data with comprehensive fallback methods
        user_data = {}
        user_source = ""

        # Method 1: From core.user_results.result.legacy
        if (
            not user_data
            and "user_results" in core
            and "result" in core["user_results"]
        ):
            user_result = core["user_results"]["result"]
            if "legacy" in user_result:
                user_data = user_result["legacy"]
                user_source = "core.user_results.result.legacy"
            elif "screen_name" in user_result or "name" in user_result:
                user_data = user_result
                user_source = "core.user_results.result (direct)"

        # Method 2: From core.user_result.result (alternative field name)
        if not user_data and "user_result" in core and "result" in core["user_result"]:
            user_result = core["user_result"]["result"]
            if "legacy" in user_result:
                user_data = user_result["legacy"]
                user_source = "core.user_result.result.legacy"

        # Method 3: From top-level user field
        if not user_data and "user" in tweet_data:
            user_obj = tweet_data["user"]
            if isinstance(user_obj, dict):
                if "legacy" in user_obj:
                    user_data = user_obj["legacy"]
                    user_source = "tweet_data.user.legacy"
                elif "result" in user_obj and "legacy" in user_obj["result"]:
                    user_data = user_obj["result"]["legacy"]
                    user_source = "tweet_data.user.result.legacy"
                elif "screen_name" in user_obj or "name" in user_obj:
                    user_data = user_obj
                    user_source = "tweet_data.user (direct)"

        # Method 4: From legacy.user (fallback for older API responses)
        if not user_data and "user" in legacy:
            user_data = legacy["user"]
            user_source = "legacy.user"

        # Method 5: Try to extract from user_id_str in legacy and cross-reference
        if not user_data and "user_id_str" in legacy:
            # Create minimal user data from available info
            user_data = {
                "id_str": legacy["user_id_str"],
                "screen_name": "",  # Will be empty but at least we have the ID
                "name": "",
            }
            user_source = "legacy.user_id_str (minimal)"

        # Extract user ID with fallbacks
        user_id = None
        if user_data:
            user_id = (
                user_data.get("id_str")
                or user_data.get("id")
                or legacy.get("user_id_str")
            )
        else:
            user_id = legacy.get("user_id_str")

        # Create comment info with robust field extraction
        comment_info = {
            "comment_id": legacy.get("id_str", ""),
            "text": legacy.get("full_text", "") or legacy.get("text", ""),
            "created_at": legacy.get("created_at", ""),
            "commenter_user_id": user_id or "",
            "commenter_username": user_data.get("screen_name", "") if user_data else "",
            "commenter_display_name": user_data.get("name", "") if user_data else "",
            "original_post_id": original_post_id,
            "in_reply_to_status_id": legacy.get("in_reply_to_status_id_str"),
            "in_reply_to_user_id": legacy.get("in_reply_to_user_id_str"),
            "favorite_count": legacy.get("favorite_count", 0),
            "retweet_count": legacy.get("retweet_count", 0),
        }

        # Validate that we have minimum required data
        if not comment_info["comment_id"]:
            return None

        return comment_info

    except Exception as e:
        return None


def process_single_post(post, user_id_lookup, username_lookup, index, total):
    """Process a single post and its comments"""
    post_result = {"post": post, "comments_found": 0, "interactions": []}

    try:
        # Get comments for this post
        comments = get_post_comments(post["post_id"])
        post_result["comments_found"] = len(comments)

        if comments:
            comments_in_master_list = 0
            for comment in comments:
                # Check if commenter is in master list
                commenter_id = comment.get("commenter_user_id")
                commenter_username = comment.get("commenter_username", "")

                # Check if commenter is in master list by user_id or username (case-insensitive)
                commenter_info = None
                if commenter_id and commenter_id in user_id_lookup:
                    commenter_info = user_id_lookup[commenter_id]
                elif (
                    commenter_username and commenter_username.lower() in username_lookup
                ):
                    commenter_info = username_lookup[commenter_username.lower()]

                if commenter_info:
                    comments_in_master_list += 1

                    # Create comment graph entry
                    graph_entry = {
                        "commenter_user_id": commenter_id,
                        "commenter_username": comment.get("commenter_username", ""),
                        "commenter_display_name": comment.get(
                            "commenter_display_name", ""
                        ),
                        "commenter_type": commenter_info.get("type", "unknown"),
                        "comment_id": comment.get("comment_id", ""),
                        "comment_text": comment.get("text", ""),
                        "comment_created_at": comment.get("created_at", ""),
                        "original_post_id": post["post_id"],
                        "original_post_author_id": post.get("user_id", ""),
                        "original_post_author_username": post.get("username", ""),
                        "original_post_text": post.get("text", ""),
                        "original_post_created_at": post.get("created_at", ""),
                        "in_reply_to_status_id": comment.get("in_reply_to_status_id"),
                        "in_reply_to_user_id": comment.get("in_reply_to_user_id"),
                        "comment_favorite_count": comment.get("favorite_count", 0),
                        "comment_retweet_count": comment.get("retweet_count", 0),
                    }

                    post_result["interactions"].append(graph_entry)

    except Exception as e:
        pass

    return post_result


def build_comment_graph(
    posts, user_id_lookup, username_lookup, days_back, max_workers=20
):
    """Build comment graph from posts and their comments with parallel processing"""
    comment_graph = []
    total_posts = len(posts)

    print(f"Processing {total_posts} posts with {max_workers} workers...")

    total_comments_found = 0
    total_interactions_added = 0
    completed_count = 0

    # Process posts in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_post = {
            executor.submit(
                process_single_post,
                post,
                user_id_lookup,
                username_lookup,
                i,
                total_posts,
            ): post
            for i, post in enumerate(posts)
        }

        # Process completed tasks as they finish
        for future in as_completed(future_to_post):
            post = future_to_post[future]
            try:
                result = future.result()
                total_comments_found += result["comments_found"]
                comment_graph.extend(result["interactions"])
                total_interactions_added += len(result["interactions"])
                completed_count += 1

                if completed_count % 20 == 0:
                    print(f"📊 {completed_count}/{total_posts} posts processed")

            except Exception as e:
                completed_count += 1

    print(f"\n✓ Processed {total_posts} posts")
    print(f"  Comments found: {total_comments_found}")
    print(f"  Interactions added: {total_interactions_added}")

    return comment_graph


def save_comment_graph(community_id, comment_graph, raw_data_dir):
    """Save comment graph to file"""

    # Calculate summary statistics
    total_comments = len(comment_graph)
    unique_commenters = len(set(entry["commenter_user_id"] for entry in comment_graph))
    unique_posts = len(set(entry["original_post_id"] for entry in comment_graph))

    # Group by commenter type
    type_breakdown = {}
    for entry in comment_graph:
        commenter_type = entry["commenter_type"]
        type_breakdown[commenter_type] = type_breakdown.get(commenter_type, 0) + 1

    # Create output data structure
    output_data = {
        "community_id": community_id,
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_comment_interactions": total_comments,
            "unique_commenters": unique_commenters,
            "unique_posts_with_comments": unique_posts,
            "commenter_type_breakdown": type_breakdown,
        },
        "comment_graph": comment_graph,
    }

    # Save to file
    filename = os.path.join(raw_data_dir, f"{community_id}_comment_graph.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(
        f"✓ Saved comment graph: {total_comments} interactions, {unique_commenters} commenters"
    )

    return filename, output_data["summary"]


def test_comment_extraction():
    """Test function to validate comment extraction improvements"""
    print("Testing comment extraction with sample data...")

    # Test TweetWithVisibilityResults wrapper
    test_data_wrapped = {
        "__typename": "TweetWithVisibilityResults",
        "tweet": {
            "legacy": {
                "id_str": "1234567890",
                "full_text": "This is a test comment",
                "created_at": "Mon Jan 01 00:00:00 +0000 2024",
                "user_id_str": "987654321",
                "favorite_count": 5,
                "retweet_count": 2,
            },
            "core": {
                "user_results": {
                    "result": {
                        "legacy": {"screen_name": "testuser", "name": "Test User"}
                    }
                }
            },
        },
    }

    # Test direct tweet data
    test_data_direct = {
        "legacy": {
            "id_str": "0987654321",
            "full_text": "Direct tweet comment",
            "created_at": "Mon Jan 02 00:00:00 +0000 2024",
            "user_id_str": "123456789",
            "favorite_count": 3,
            "retweet_count": 1,
        },
        "core": {
            "user_results": {
                "result": {
                    "legacy": {"screen_name": "directuser", "name": "Direct User"}
                }
            }
        },
    }

    # Test extraction
    print("Testing wrapped tweet extraction:")
    result1 = extract_comment_info(test_data_wrapped, "original_post_123")
    if result1:
        print(f"  Success: {result1['commenter_username']} - {result1['text'][:50]}...")
    else:
        print("  Failed to extract wrapped tweet")

    print("Testing direct tweet extraction:")
    result2 = extract_comment_info(test_data_direct, "original_post_456")
    if result2:
        print(f"  Success: {result2['commenter_username']} - {result2['text'][:50]}...")
    else:
        print("  Failed to extract direct tweet")

    return result1 is not None and result2 is not None


def main():
    """Main function - build and analyze comment graph"""
    global rate_limiter, request_count, start_time

    # Initialize to avoid unbound variable in exception handler
    raw_data_dir = "./raw"

    try:
        # Load configuration
        config = load_config()
        if not config:
            return

        # Initialize rate limiter with config values
        requests_per_second = config["rate_limiting"]["requests_per_second"]
        rate_limiter = RateLimiter(requests_per_second)

        # Reset counters
        request_count = 0
        start_time = None

        print(f"Comment Graph Fetcher")
        print(f"Rate limiting: {requests_per_second} requests/second")
        print(f"=" * 60)

        community_ids = config["communities"]["ids"]
        raw_data_dir = config["output"]["raw_data_dir"]
        days_back = config["data"]["days_back"]
        community_delay = config["rate_limiting"]["community_delay"]

        for i, community_id in enumerate(community_ids):
            print(f"\n{'=' * 60}")
            print(f"Community {i + 1}/{len(community_ids)}: {community_id}")
            print(f"{'=' * 60}")

            # Load master list from all sources
            try:
                master_list, user_id_lookup, username_lookup = load_master_list(
                    community_id, raw_data_dir
                )
                if not master_list:
                    print(f"⚠️  No users found, skipping")
                    continue
            except Exception as e:
                print(f"❌ Error loading data: {e}")
                continue

            # Get community posts
            try:
                posts = get_community_posts(community_id, days_back)
                if not posts:
                    print(f"⚠️  No posts found")
                    continue
            except Exception as e:
                print(f"❌ Error fetching posts: {e}")
                continue

            # Build comment graph
            try:
                comment_graph = build_comment_graph(
                    posts, user_id_lookup, username_lookup, days_back
                )

                save_comment_graph(community_id, comment_graph, raw_data_dir)
            except Exception as e:
                print(f"❌ Error building graph: {e}")
                continue

            if i < len(community_ids) - 1:
                time.sleep(community_delay)

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'=' * 60}")
            print(f"✅ COMPLETE")
            print(f"{'=' * 60}")
            print(f"Communities: {len(community_ids)}")
            print(f"API requests: {request_count} ({avg_rate:.2f}/sec)")
            print(f"Time: {total_time:.1f}s")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_passed = test_comment_extraction()
        print(f"\nTest {'PASSED' if test_passed else 'FAILED'}")
    else:
        main()

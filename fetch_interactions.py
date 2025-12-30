#!/usr/bin/env python3
"""
Seed Users Interactions Fetcher

This script fetches all interactions from users in the seed followings master list:
1. Loads raw/[seed_graph]_followings.json (created by fetch_followings.py)
2. Sorts users by user_id (lowest to highest)
3. Fetches all posts/tweets and replies from each user in the master list
4. Applies days_back and post_limit from config.toml
5. Saves each batch to raw/[seed_graph]_[first_user_id]_[last_user_id].json
6. Clears batch data from memory after saving

Uses endpoints:
- /twitter/user/last_tweets for user timeline posts (includes original posts, retweets, quotes, and replies)

Rate limited to 1,000 requests per second to comply with API limits.
"""

import http.client
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

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
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Config is in the same directory as the script
        config_path = os.path.join(script_dir, "config.toml")
        with open(config_path, "r") as f:
            return toml.load(f)
    except FileNotFoundError:
        print("Error: config.toml not found")
        return None
    except Exception as e:
        print(f"Error loading config: {e}")
        return None


def get_api_key():
    """Get API key from environment with proper formatting"""
    api_key = os.getenv("TWITTER_API_KEY")

    if not api_key:
        raise ValueError("TWITTER_API_KEY not found in environment variables")

    # Remove surrounding quotes if present
    if api_key.startswith('"') and api_key.endswith('"'):
        api_key = api_key[1:-1]
    elif api_key.startswith("'") and api_key.endswith("'"):
        api_key = api_key[1:-1]

    if not api_key.strip():
        raise ValueError("TWITTER_API_KEY is empty after cleaning")

    return api_key


def make_request(endpoint, params=None, max_retries=3):
    """Make HTTP request to twitterapi.io with rate limiting and exponential backoff"""
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

        # Log progress every 50 requests
        if request_count % 50 == 0:
            elapsed = time.time() - start_time
            rate = request_count / elapsed if elapsed > 0 else 0
            print(
                f"API Requests: {request_count}, Rate: {rate:.2f}/sec, Elapsed: {elapsed:.1f}s"
            )

        try:
            conn = http.client.HTTPSConnection("api.twitterapi.io")

            headers = {
                "X-API-Key": get_api_key(),
            }

            # Build URL with query parameters
            if params:
                query_string = "&".join([f"{k}={v}" for k, v in params.items()])
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
                    backoff_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
                    print(
                        f"Rate limit hit, waiting {backoff_time}s before retry (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(backoff_time)
                    continue
                else:
                    print(f"Rate limit exceeded, max retries reached")
                    return None
            elif res.status >= 500:  # Server errors
                if attempt < max_retries - 1:
                    backoff_time = 2**attempt
                    print(
                        f"Server error {res.status}, retrying in {backoff_time}s (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(backoff_time)
                    continue
                else:
                    print(f"Error: HTTP {res.status} - {data.decode('utf-8')}")
                    return None
            else:
                print(f"Error: HTTP {res.status} - {data.decode('utf-8')}")
                return None

        except Exception as e:
            if attempt < max_retries - 1:
                backoff_time = 2**attempt
                print(
                    f"Request failed: {str(e)}, retrying in {backoff_time}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(backoff_time)
                continue
            else:
                print(f"Request failed after {max_retries} attempts: {str(e)}")
                return None

    return None


def load_seed_followings(raw_data_dir):
    """Load the seed followings master list for the seed_graph from config"""
    # Load config to get seed_graph name
    config = load_config()
    if not config:
        return None, None

    # Get seed_graph name from config
    seed_graph_config = config.get("seed_graph", {})
    if not seed_graph_config:
        print("Error: No [seed_graph] section found in config.toml")
        return None, None

    # Get the first (and typically only) community name from seed_graph
    seed_graph_name = list(seed_graph_config.keys())[0] if seed_graph_config else None

    if not seed_graph_name:
        print("Error: No seed_graph name found in config.toml [seed_graph] section")
        return None, None

    print(f"Using seed_graph: {seed_graph_name}")

    # Load the followings file: raw/[seed_graph]_followings.json
    filename = os.path.join(raw_data_dir, f"{seed_graph_name}_followings.json")

    if not os.path.exists(filename):
        print(f"Error: Seed followings file not found: {filename}")
        print(f"Please run fetch_followings.py first")
        return None, None

    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)

        master_list = data.get("master_list", [])
        seed_users = data.get("seed_users", [])

        # Sort master_list by user_id (as integer, from lowest to highest)
        master_list_sorted = sorted(
            master_list,
            key=lambda u: int(u.get("user_id", 0))
            if u.get("user_id", "").isdigit()
            else 0,
        )

        print(f"Loaded seed followings from {filename}")
        print(f"  - Seed graph: {seed_graph_name}")
        print(f"  - Seed users: {len(seed_users)}")
        print(f"  - Total users in master list: {len(master_list_sorted)}")

        return master_list_sorted, seed_graph_name

    except Exception as e:
        print(f"Error loading seed followings: {e}")
        return None, None


def is_post_within_days(created_at_str, days_back):
    """Check if post is within the specified days back"""
    try:
        if not created_at_str:
            return False

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


def extract_post_data(tweet):
    """Extract relevant data from a tweet using new API format"""
    if not tweet or not isinstance(tweet, dict):
        return None

    try:
        # New API format is much simpler
        author = tweet.get("author", {})
        if not isinstance(author, dict):
            author = {}

        # Determine post type
        is_reply = tweet.get("isReply", False)
        is_retweet = "retweeted_tweet" in tweet and tweet.get("retweeted_tweet")
        is_quote = "quoted_tweet" in tweet and tweet.get("quoted_tweet")

        # Basic post information
        extracted_data = {
            "post_id": tweet.get("id", ""),
            "text": tweet.get("text", ""),
            "created_at": tweet.get("createdAt", ""),
            "user_id": author.get("id", ""),
            "username": author.get("userName", ""),
            "is_retweet": is_retweet,
            "is_reply": is_reply,
            "is_quote": is_quote,
            "reply_to_post_id": tweet.get("inReplyToId"),
            "reply_to_user_id": tweet.get("inReplyToUserId"),
            "reply_to_username": tweet.get("inReplyToUsername"),
            "retweeted_post_id": None,
            "quoted_post_id": None,
            "original_post_creator_id": None,
            "original_post_creator_username": None,
        }

        # Extract retweeted post data if available
        if is_retweet:
            retweeted = tweet.get("retweeted_tweet", {})
            if retweeted:
                rt_author = retweeted.get("author", {})
                extracted_data["retweeted_post_id"] = retweeted.get("id")
                extracted_data["original_post_creator_id"] = rt_author.get("id")
                extracted_data["original_post_creator_username"] = rt_author.get(
                    "userName"
                )

                extracted_data["retweeted_post"] = {
                    "post_id": retweeted.get("id", ""),
                    "text": retweeted.get("text", ""),
                    "created_at": retweeted.get("createdAt", ""),
                    "user_id": rt_author.get("id", ""),
                    "username": rt_author.get("userName", ""),
                    "user_display_name": rt_author.get("name", ""),
                }

        # Extract quoted post data if available
        if is_quote:
            quoted = tweet.get("quoted_tweet", {})
            if quoted:
                qt_author = quoted.get("author", {})
                extracted_data["quoted_post_id"] = quoted.get("id")
                extracted_data["original_post_creator_id"] = qt_author.get("id")
                extracted_data["original_post_creator_username"] = qt_author.get(
                    "userName"
                )

                extracted_data["quoted_post"] = {
                    "post_id": quoted.get("id", ""),
                    "text": quoted.get("text", ""),
                    "created_at": quoted.get("createdAt", ""),
                    "user_id": qt_author.get("id", ""),
                    "username": qt_author.get("userName", ""),
                    "user_display_name": qt_author.get("name", ""),
                }

        return extracted_data

    except Exception as e:
        print(f"Error extracting post data: {e}")
        return None


def get_user_tweets(username, user_id, days_back, max_tweets=1000):
    """Get user's tweets and replies using the /twitter/user/last_tweets endpoint"""
    content = []
    cursor = None
    page = 0

    print(f"  Fetching tweets for @{username} (ID: {user_id})...")

    while len(content) < max_tweets and page < 10:  # Limit pages
        page += 1

        params = {"userId": user_id, "includeReplies": "true"}
        if cursor:
            params["cursor"] = cursor

        response = make_request("/twitter/user/last_tweets", params)

        if not response:
            print(f"    No response from API for @{username}")
            break

        # Check API response status
        status = response.get("status") if isinstance(response, dict) else None
        if status != "success":
            message = (
                response.get("message", "Unknown error")
                if isinstance(response, dict)
                else "Invalid response format"
            )
            print(f"    API error: {message}")
            break

        # Extract tweets from new API format (inside data object)
        found_content = False

        try:
            # Tweets are nested inside data object
            if not isinstance(response, dict):
                print(f"    Invalid response type for @{username}: {type(response)}")
                break

            data = response.get("data")
            if not data or not isinstance(data, dict):
                print(f"    No data in response for @{username}")
                break

            tweets = data.get("tweets", [])

            if not tweets:
                break

            for tweet in tweets:
                # Check if within date range
                created_at = tweet.get("createdAt")

                if is_post_within_days(created_at, days_back):
                    extracted = extract_post_data(tweet)
                    if extracted:
                        content.append(extracted)
                        found_content = True
                else:
                    # If we hit content outside date range, stop
                    print(f"    Reached content outside date range for @{username}")
                    return content

        except Exception as e:
            print(
                f"    Error parsing response for @{username}: {type(e).__name__}: {e}"
            )
            break

        if not found_content:
            break

        # Check for next page
        if response.get("has_next_page"):
            cursor = response.get("next_cursor")
        else:
            cursor = None

        if not cursor:
            break

        print(
            f"    Page {page}: Found {len([c for c in content if c]) if content else 0} posts"
        )

    print(f"  Total tweets: {len(content)} posts for @{username}")
    return content


def fetch_user_interactions(user, days_back, post_limit):
    """Fetch all interactions for a single user"""
    username = user.get("username", "")
    user_id = user.get("user_id", "")
    if not username or not user_id:
        print(f"  Skipping user with missing username or ID")
        return None

    print(f"\nProcessing user: @{username}")

    user_data = {
        "username": username,
        "user_id": user_id,
        "display_name": user.get("display_name", ""),
        "posts": [],
        "replies": [],
    }

    # Get user's tweets and replies (now combined in one endpoint)
    all_content = get_user_tweets(username, user_id, days_back, max_tweets=post_limit)

    # Separate posts and replies based on is_reply flag
    for item in all_content:
        if item:
            if item.get("is_reply"):
                user_data["replies"].append(item)
            else:
                user_data["posts"].append(item)

    print(
        f"  Found {len(user_data['posts'])} posts and {len(user_data['replies'])} replies"
    )

    return user_data


def get_processed_user_id_ranges_from_batch_files(raw_data_dir, seed_graph_name):
    """Get list of (first_user_id, last_user_id) ranges by parsing batch filenames only"""
    import glob

    pattern = os.path.join(raw_data_dir, f"{seed_graph_name}_*_*.json")
    matching_files = glob.glob(pattern)

    # Exclude the followings file from matching
    followings_file = os.path.join(raw_data_dir, f"{seed_graph_name}_followings.json")

    processed_ranges = []

    for file_path in matching_files:
        if file_path == followings_file:
            continue

        # Extract first and last user IDs from filename
        # Format: [seed_graph]_[first_user_id]_[last_user_id].json
        basename = os.path.basename(file_path)
        # Remove .json extension
        name_without_ext = basename.rsplit(".json", 1)[0]
        # Remove seed_graph prefix
        prefix = f"{seed_graph_name}_"
        if name_without_ext.startswith(prefix):
            rest = name_without_ext[len(prefix) :]
            # Split by underscore to get first and last user IDs
            parts = rest.rsplit("_", 1)
            if len(parts) == 2:
                first_id, last_id = parts
                if first_id.isdigit() and last_id.isdigit():
                    processed_ranges.append((int(first_id), int(last_id)))

    return processed_ranges


def is_user_in_processed_ranges(user_id, processed_ranges):
    """Check if a user_id falls within any of the processed ranges"""
    user_id_int = int(user_id) if str(user_id).isdigit() else 0
    for first_id, last_id in processed_ranges:
        if first_id <= user_id_int <= last_id:
            return True
    return False


def save_batch_interactions(
    batch_interactions, raw_data_dir, seed_graph_name, first_user_id, last_user_id
):
    """Save a batch of interactions data to JSON file and return filename"""
    filename = os.path.join(
        raw_data_dir, f"{seed_graph_name}_{first_user_id}_{last_user_id}.json"
    )
    os.makedirs(raw_data_dir, exist_ok=True)

    output_data = {
        "timestamp": datetime.now().isoformat(),
        "seed_graph": seed_graph_name,
        "first_user_id": first_user_id,
        "last_user_id": last_user_id,
        "total_users": len(batch_interactions),
        "users": batch_interactions,
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    # Calculate stats
    total_posts = sum(len(u.get("posts", [])) for u in batch_interactions)
    total_replies = sum(len(u.get("replies", [])) for u in batch_interactions)

    print(f"\n✓ Saved batch to: {filename}")
    print(f"  - Users in batch: {len(batch_interactions)}")
    print(f"  - Total posts: {total_posts}")
    print(f"  - Total replies: {total_replies}")

    return filename


def main():
    """Main function - fetch seed users interactions and save to JSON files"""
    global rate_limiter, request_count, start_time

    # Initialize to avoid unbound variable in exception handler
    raw_data_dir = "./raw"

    try:
        # Load configuration
        config = load_config()
        if not config:
            return

        # Initialize rate limiter with config values
        requests_per_second = config.get("rate_limiting", {}).get(
            "requests_per_second", 1000
        )
        rate_limiter = RateLimiter(requests_per_second)

        # Get max_parallel parameter (default to 4 if not in config)
        max_parallel = config.get("rate_limiting", {}).get("max_parallel_requests", 4)

        # Reset counters
        request_count = 0
        start_time = None

        print(f"Seed Users Interactions Fetcher")
        print(f"Rate limiting: {requests_per_second} requests/second")
        print(f"Max parallel users: {max_parallel}")
        print(f"=" * 60)

        # Get raw_data_dir and make it relative to script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        raw_data_dir_config = config.get("output", {}).get("raw_data_dir", "./raw")
        raw_data_dir = os.path.join(script_dir, raw_data_dir_config.lstrip("./"))
        days_back = config.get("data", {}).get("days_back", 365)
        post_limit_per_user = config.get("data", {}).get("post_limit_per_user", 500)

        print(f"Configuration:")
        print(f"  - Days back: {days_back}")
        print(f"  - Post limit per user: {post_limit_per_user}")
        print(f"  - Raw data directory: {raw_data_dir}")

        # Load seed followings master list (already sorted by user_id)
        master_list, seed_graph_name = load_seed_followings(raw_data_dir)
        if not master_list or not seed_graph_name:
            print("Error: Could not load seed followings")
            return

        # Get already processed user ID ranges from existing batch filenames
        processed_ranges = get_processed_user_id_ranges_from_batch_files(
            raw_data_dir, seed_graph_name
        )

        # Filter out already processed users
        remaining_users = [
            u
            for u in master_list
            if not is_user_in_processed_ranges(u.get("user_id", ""), processed_ranges)
        ]

        already_processed_count = len(master_list) - len(remaining_users)
        print(f"\nTotal users in master list: {len(master_list)}")
        print(
            f"Already processed (from {len(processed_ranges)} batch files): {already_processed_count}"
        )
        print(f"Remaining to process: {len(remaining_users)}")

        if not remaining_users:
            print("All users have been processed!")
            return

        # Process remaining users in parallel batches
        batch_size = max_parallel
        total_batches_processed = 0
        save_every_n_batches = 10

        # Accumulator for multiple batches before saving
        accumulated_interactions = []
        accumulated_first_user_id = None
        accumulated_last_user_id = None
        batches_since_last_save = 0

        for batch_start in range(0, len(remaining_users), batch_size):
            batch_end = min(batch_start + batch_size, len(remaining_users))
            batch_users = remaining_users[batch_start:batch_end]

            # Get first and last user IDs for this batch (for filename)
            first_user_id = batch_users[0].get("user_id", "unknown")
            last_user_id = batch_users[-1].get("user_id", "unknown")

            print(
                f"\nProcessing batch {batch_start // batch_size + 1}: users {batch_start + 1}-{batch_end} of {len(remaining_users)}"
            )
            print(f"  User ID range: {first_user_id} - {last_user_id}")

            # Process batch in parallel
            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                # Submit all users in batch
                future_to_user = {
                    executor.submit(
                        fetch_user_interactions, user, days_back, post_limit_per_user
                    ): user
                    for user in batch_users
                }

                # Collect results as they complete
                for future in as_completed(future_to_user):
                    user = future_to_user[future]
                    try:
                        user_data = future.result()
                        if user_data:  # Only add if we got valid data
                            accumulated_interactions.append(user_data)
                            print(f"  ✓ Completed @{user.get('username', 'unknown')}")
                    except Exception as e:
                        print(
                            f"  ✗ Error processing user @{user.get('username', 'unknown')}: {e}"
                        )
                        # Continue with other users even if one fails
                        continue

            # Track user ID range for accumulated batches
            if accumulated_first_user_id is None:
                accumulated_first_user_id = first_user_id
            accumulated_last_user_id = last_user_id
            batches_since_last_save += 1
            total_batches_processed += 1

            # Save every N batches or on the last batch
            is_last_batch = batch_end >= len(remaining_users)
            if batches_since_last_save >= save_every_n_batches or is_last_batch:
                if accumulated_interactions:
                    save_batch_interactions(
                        accumulated_interactions,
                        raw_data_dir,
                        seed_graph_name,
                        accumulated_first_user_id,
                        accumulated_last_user_id,
                    )
                    # Clear accumulated data from memory
                    accumulated_interactions = []
                    accumulated_first_user_id = None
                    accumulated_last_user_id = None
                    batches_since_last_save = 0

            print(
                f"Progress: {batch_end}/{len(remaining_users)} remaining users processed"
            )

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'=' * 60}")
            print(f"INTERACTIONS FETCH COMPLETE")
            print(f"{'=' * 60}")
            print(f"Batches saved: {total_batches_processed}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

        print(f"\nBatch files saved in {raw_data_dir}:")
        print(f"- {seed_graph_name}_[first_user_id]_[last_user_id].json")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

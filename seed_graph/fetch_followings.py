#!/usr/bin/env python3
"""
Seed Users Following Fetcher

This script fetches followings of seed users and creates a master list:
1. Loads seed usernames from config.toml [seed_users] section
2. For each seed user, fetches all their followings
3. Creates a master list containing:
   - Seed users themselves
   - All users they follow (deduplicated)
4. Saves to raw/{seed_user_id}_seed_followings.json

Uses endpoints:
- /following-ids to get following IDs
- /get-users-v2 to get user information for multiple user IDs (batch of 50)
- /user to get user information by username (for seed users)

Rate limited to 10 requests per second to comply with API limits.
"""

import http.client
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import toml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class RateLimiter:
    """Rate limiter to ensure we don't exceed API rate limits"""

    def __init__(self, requests_per_second=10):
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


def get_api_key():
    """Get API key from environment with proper formatting"""
    api_key = os.getenv("RAPIDAPI_KEY")

    if not api_key:
        raise ValueError("RAPIDAPI_KEY not found in environment variables")

    # Remove surrounding quotes if present
    if api_key.startswith('"') and api_key.endswith('"'):
        api_key = api_key[1:-1]
    elif api_key.startswith("'") and api_key.endswith("'"):
        api_key = api_key[1:-1]

    if not api_key.strip():
        raise ValueError("RAPIDAPI_KEY is empty after cleaning")

    return api_key


def make_request(endpoint, params="", max_retries=3):
    """Make HTTP request to RapidAPI with rate limiting and exponential backoff"""
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
            conn = http.client.HTTPSConnection("twitter241.p.rapidapi.com")

            headers = {
                "x-rapidapi-key": get_api_key(),
                "x-rapidapi-host": "twitter241.p.rapidapi.com",
            }

            full_endpoint = f"{endpoint}?{params}" if params else endpoint

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
                    error_msg = data.decode("utf-8") if data else "No response data"
                    print(f"Error: HTTP {res.status} - {error_msg}")
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


def get_user_info(username):
    """Get user profile information by username using /user endpoint"""
    print(f"  Fetching user info for @{username}")

    params = f"username={username}"
    response = make_request("/user", params)

    if not response:
        print(f"  Could not fetch user info for @{username}")
        return None

    # Check for error messages in response
    if "message" in response or "error" in response:
        error_msg = response.get("message") or response.get("error")
        print(f"  API Error: {error_msg}")
        print(f"  User @{username} may not exist or be inaccessible")
        return None

    # Try different response formats
    user_result = None

    # Format 1: {"user": {"result": {...}}}
    if "user" in response and "result" in response.get("user", {}):
        user_result = response["user"]["result"]
    # Format 2: {"result": {"data": {"user": {"result": {...}}}}}
    elif "result" in response and "data" in response.get("result", {}):
        data = response["result"]["data"]
        # Check for user.result structure
        if "user" in data and "result" in data.get("user", {}):
            user_result = data["user"]["result"]
        # Check for users array structure
        elif "users" in data and len(data["users"]) > 0:
            user_result = data["users"][0].get("result")
    # Format 3: {"data": {"user": {...}}}
    elif "data" in response and "user" in response.get("data", {}):
        user_result = response["data"]["user"]
    # Format 4: Direct user object
    elif "rest_id" in response or "legacy" in response:
        user_result = response

    if not user_result:
        print(f"  No user data in response for @{username}")
        print(f"  Response keys: {list(response.keys())}")
        if response:
            print(f"  Sample response (first 300 chars): {str(response)[:300]}")
        return None

    # Extract user information from legacy format
    legacy = user_result.get("legacy", {})

    # Check if user is suspended or doesn't exist
    if not legacy:
        print(f"  No legacy user data found for @{username}")
        print(f"  User may be suspended, deleted, or username is incorrect")
        return None

    # Get rest_id from various possible locations
    rest_id = (
        user_result.get("rest_id") or user_result.get("id_str") or user_result.get("id")
    )

    user_info = {
        "user_id": rest_id,
        "username": legacy.get("screen_name", username),
        "display_name": legacy.get("name", username),
        "followers_count": legacy.get("followers_count", 0),
        "following_count": legacy.get("friends_count", 0),
        "verified": legacy.get("verified", False),
        "verified_type": user_result.get("verification_info", {})
        .get("reason", {})
        .get("description", {})
        .get("text", ""),
        "description": legacy.get("description", ""),
        "location": legacy.get("location", ""),
        "created_at": legacy.get("created_at", ""),
    }

    if user_info["user_id"]:
        print(
            f"  ✓ Got user info: ID={user_info['user_id']}, followers={user_info['followers_count']}"
        )
        return user_info
    else:
        print(f"  Could not extract user_id from response for @{username}")
        print(f"  User data may be incomplete or corrupted")
        return None


def fetch_single_batch(batch, batch_num, total_batches):
    """Fetch user info for a single batch of user IDs

    Args:
        batch: List of user IDs to fetch
        batch_num: Current batch number
        total_batches: Total number of batches

    Returns:
        List of user info dictionaries
    """
    batch_users = []

    # Join user IDs with commas and URL encode
    users_param = ",".join(str(uid) for uid in batch)
    params = f"users={users_param}"

    print(f"    Batch {batch_num}/{total_batches}: Fetching {len(batch)} users...")

    response = make_request("/get-users-v2", params)

    if not response:
        print(f"    Batch {batch_num}: No response received")
        return batch_users

    # Response format: {"result": [...]}
    result = response.get("result", [])

    if not result:
        print(f"    Batch {batch_num}: No users in response")
        return batch_users

    # Process each user in the batch
    for user_data in result:
        try:
            user_info = {
                "user_id": str(user_data.get("id_str") or user_data.get("id", "")),
                "username": user_data.get("screen_name", ""),
                "display_name": user_data.get("name", ""),
                "followers_count": user_data.get("followers_count", 0),
                "following_count": user_data.get("friends_count", 0),
                "verified": user_data.get("verified", False),
                "verified_type": "",  # Not directly in v2 response
                "description": user_data.get("description", ""),
                "location": user_data.get("location", ""),
                "created_at": user_data.get("created_at", ""),
            }

            # Only add users with valid usernames
            if user_info["username"]:
                batch_users.append(user_info)
        except Exception as e:
            print(f"    Batch {batch_num}: Error parsing user data: {e}")
            continue

    print(f"    Batch {batch_num}: Got {len(result)} users")
    return batch_users


def get_users_info_batch(user_ids, batch_size=50, max_parallel=4):
    """Fetch user info for multiple user IDs using /get-users-v2 endpoint in parallel

    Args:
        user_ids: List of user IDs to fetch
        batch_size: Number of users to fetch per request (max 50)
        max_parallel: Maximum number of parallel requests

    Returns:
        List of user objects with full profile information
    """
    users_info = []
    total_ids = len(user_ids)
    total_batches = (total_ids + batch_size - 1) // batch_size

    # Create batches
    batches = []
    for i in range(0, total_ids, batch_size):
        batch = user_ids[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        batches.append((batch, batch_num, total_batches))

    print(
        f"  Fetching {total_ids} users in {total_batches} batches (max {max_parallel} parallel)..."
    )

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        # Submit all batches
        future_to_batch = {
            executor.submit(
                fetch_single_batch, batch, batch_num, total_batches
            ): batch_num
            for batch, batch_num, total_batches in batches
        }

        # Collect results as they complete
        for future in as_completed(future_to_batch):
            batch_num = future_to_batch[future]
            try:
                batch_users = future.result()
                users_info.extend(batch_users)
            except Exception as e:
                print(f"    Batch {batch_num}: Exception occurred: {e}")
                continue

    print(f"  ✓ Successfully fetched {len(users_info)}/{total_ids} users")
    return users_info


def get_user_followings(username, max_following=10000, max_parallel=4):
    """Get list of users that a user is following using RapidAPI"""
    print(f"\nFetching followings for @{username}")

    following_ids_set = set()  # Track unique IDs
    cursor = None
    page = 0
    max_pages = 100  # Reasonable limit

    # First, get the following IDs
    while page < max_pages:
        page += 1

        # RapidAPI uses username and count parameters
        params = f"username={username}&count=500"
        if cursor:
            params += f"&cursor={cursor}"

        print(f"  Page {page}: Fetching following IDs...")

        response = make_request("/following-ids", params)

        if not response:
            print(f"  Page {page}: No response received")
            break

        try:
            # Extract following IDs from response - ids are at top level
            # Response format: {"ids": [...], "next_cursor": ..., "next_cursor_str": ...}
            new_ids = response.get("ids", [])

            if not new_ids:
                print(f"  Page {page}: No following IDs in response")
                break

            before_count = len(following_ids_set)
            following_ids_set.update(new_ids)
            after_count = len(following_ids_set)
            new_count = after_count - before_count

            print(
                f"  Page {page}: Got {len(new_ids)} IDs, {new_count} new unique (total: {after_count})"
            )

            # Check for next page
            next_cursor = response.get("next_cursor")
            if next_cursor and next_cursor != cursor:
                cursor = next_cursor
            else:
                print(f"  Page {page}: No more pages")
                break

            if len(following_ids_set) >= max_following:
                print(f"  Reached ID limit of {max_following}")
                break

        except Exception as e:
            print(f"  Page {page}: Error parsing following IDs for @{username}: {e}")
            import traceback

            traceback.print_exc()
            break

    following_ids_list = list(following_ids_set)
    print(f"  ✓ Found {len(following_ids_list)} unique following IDs for @{username}")

    # Fetch full user info for all following IDs using batch endpoint
    print(f"  Fetching full user profiles...")
    following_users = get_users_info_batch(
        following_ids_list, batch_size=50, max_parallel=max_parallel
    )

    if len(following_users) < len(following_ids_list):
        missing = len(following_ids_list) - len(following_users)
        print(
            f"  ⚠️  Warning: {missing} users could not be fetched (suspended/deleted accounts)"
        )

    return following_users


def build_master_list(seed_usernames, max_parallel=4):
    """Build master list from seed users and their followings"""
    print(f"\n{'=' * 60}")
    print(f"Building master list from {len(seed_usernames)} seed users")
    print(f"{'=' * 60}")

    master_dict = {}  # Use dict to avoid duplicates, keyed by user_id
    seed_users_info = []  # Track seed users separately

    for i, username in enumerate(seed_usernames):
        print(f"\n[{i + 1}/{len(seed_usernames)}] Processing seed user: @{username}")

        try:
            # Get followings for this seed user
            following_users = get_user_followings(username, max_parallel=max_parallel)

            # Add this seed user to master list (we need to mark them as seed users)
            # For now, we'll fetch their info when we get their followings
            # The seed user might appear in their followings if they follow themselves
            # or we can add them manually

            # Add all following users to master dict
            for user in following_users:
                user_id = user.get("user_id")
                if user_id and user_id not in master_dict:
                    # Mark if this is a seed user
                    user["type"] = "following"
                    master_dict[user_id] = user

            # Mark this username as a seed user (in case they appear in followings)
            # If not, we'll need to get their info separately
            found_seed_user = False
            for user_id, user in master_dict.items():
                if user.get("username", "").lower() == username.lower():
                    user["type"] = "seed"
                    seed_users_info.append(user)
                    found_seed_user = True
                    break

            if not found_seed_user:
                # Seed user not in their own followings, fetch their info
                print(f"  Seed user @{username} not in their own followings list")
                print(f"  Fetching real user info from API...")

                seed_user_info = get_user_info(username)

                if seed_user_info and seed_user_info.get("user_id"):
                    # Got real user info
                    seed_user_info["type"] = "seed"
                    seed_user_info["following_count"] = len(following_users)
                    seed_user_id = seed_user_info["user_id"]
                    print(f"  ✓ Added seed user with real ID: {seed_user_id}")
                    master_dict[seed_user_id] = seed_user_info
                    seed_users_info.append(seed_user_info)
                else:
                    # Skip this seed user if we can't get their real ID
                    print(f"  ❌ Could not get real user ID for @{username}, skipping")
                    continue

        except Exception as e:
            print(f"  Error processing seed user @{username}: {e}")
            import traceback

            traceback.print_exc()
            continue

    master_list = list(master_dict.values())

    print(f"\n{'=' * 60}")
    print(f"Master list created:")
    print(f"  - Seed users: {len(seed_users_info)}")
    print(f"  - Total unique users: {len(master_list)}")
    print(f"  - Following users: {len(master_list) - len(seed_users_info)}")
    print(f"{'=' * 60}")

    return master_list, seed_users_info


def save_seed_followings(master_list, seed_users_info, raw_data_dir):
    """Save master list to JSON file"""
    # Get the first seed user's ID for filename prefix
    seed_user_id = seed_users_info[0].get("user_id") if seed_users_info else "unknown"
    filename = os.path.join(raw_data_dir, f"{seed_user_id}_seed_followings.json")
    os.makedirs(raw_data_dir, exist_ok=True)

    output_data = {
        "timestamp": datetime.now().isoformat(),
        "seed_users_count": len(seed_users_info),
        "total_users_count": len(master_list),
        "seed_users": [
            {
                "username": user.get("username"),
                "user_id": user.get("user_id"),
                "display_name": user.get("display_name"),
            }
            for user in seed_users_info
        ],
        "master_list": master_list,
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Saved master list to: {filename}")
    print(f"  - Seed users: {len(seed_users_info)}")
    print(f"  - Total users: {len(master_list)}")

    return filename


def main():
    """Main function - fetch seed users' followings and create master list"""
    global rate_limiter, request_count, start_time

    # Initialize to avoid unbound variable in exception handler
    raw_data_dir = "./raw/seed"

    try:
        # Load configuration
        config = load_config()
        if not config:
            return

        # Get seed user IDs from config [seed_graph] section
        seed_graph_config = config.get("seed_graph", {})
        if not seed_graph_config:
            print("Error: No [seed_graph] section found in config.toml")
            return

        # Collect all user IDs from all communities
        seed_user_ids = []
        for community_name, user_ids in seed_graph_config.items():
            if isinstance(user_ids, list):
                seed_user_ids.extend(str(uid) for uid in user_ids)

        if not seed_user_ids:
            print("Error: No seed user IDs found in config.toml [seed_graph] section")
            return

        # Filter out any invalid entries
        seed_user_ids = [uid for uid in seed_user_ids if uid and uid.isdigit()]

        if not seed_user_ids:
            print("Error: No valid seed user IDs found")
            return

        print(f"Fetching user info for {len(seed_user_ids)} seed user IDs...")
        # Fetch user info (including usernames) from user IDs
        seed_users_info_list = get_users_info_batch(
            seed_user_ids, batch_size=50, max_parallel=4
        )

        if not seed_users_info_list:
            print("Error: Could not fetch user info for seed user IDs")
            return

        # Extract usernames from fetched user info
        seed_usernames = [
            user.get("username")
            for user in seed_users_info_list
            if user.get("username")
        ]

        if not seed_usernames:
            print("Error: No valid usernames found for seed user IDs")
            return

        print(f"Resolved {len(seed_usernames)} usernames from user IDs")

        # Initialize rate limiter with config values (default to 10 for RapidAPI)
        requests_per_second = config.get("rate_limiting", {}).get(
            "requests_per_second", 10
        )
        rate_limiter = RateLimiter(requests_per_second)

        # Get max parallel requests from config (default to 4)
        max_parallel = config.get("rate_limiting", {}).get("max_parallel_requests", 4)

        # Reset counters
        request_count = 0
        start_time = None

        print(f"Seed Users Following Fetcher")
        print(f"Rate limiting: {requests_per_second} requests/second")
        print(f"Max parallel requests: {max_parallel}")
        print(
            f"Seed users to process: {len(seed_usernames)} (from {len(seed_user_ids)} user IDs)"
        )
        print(f"=" * 60)

        # Get raw_data_dir and make it relative to project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.join(script_dir, "..")
        raw_data_dir_config = config.get("output", {}).get("raw_data_dir", "./raw")
        raw_data_dir = os.path.join(
            project_root, raw_data_dir_config.lstrip("./"), "seed"
        )

        # Build master list from seed users' followings
        master_list, seed_users_info = build_master_list(
            seed_usernames, max_parallel=max_parallel
        )

        # Save to file
        if master_list:
            save_seed_followings(master_list, seed_users_info, raw_data_dir)
        else:
            print("No data collected")

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'=' * 60}")
            print(f"SEED FOLLOWINGS FETCH COMPLETE")
            print(f"{'=' * 60}")
            print(f"Seed users processed: {len(seed_usernames)}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

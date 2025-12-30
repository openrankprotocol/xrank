#!/usr/bin/env python3
"""
Extended Followings Fetcher

This script fetches followings for all users in the master list and filters them:
1. Loads raw/[seed_graph]_followings.json (created by fetch_followings.py)
2. Creates a master_list set of all user IDs (e.g., 61k for optimism_followings.json)
3. Goes through each user in master_list and fetches their followings
4. Keeps only followings that are also in the master_list
5. Saves to raw/[seed_graph]_extended_followings.json

Uses endpoints:
- /following-ids to get following IDs for each user

Rate limited to comply with API limits.
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
        self.min_interval = 1.0 / requests_per_second
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

    if start_time is None:
        start_time = time.time()

    for attempt in range(max_retries):
        if rate_limiter:
            rate_limiter.wait_for_token()

        request_count += 1

        if request_count % 100 == 0:
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
            elif res.status == 429:
                if attempt < max_retries - 1:
                    backoff_time = 2**attempt
                    print(
                        f"Rate limit hit, waiting {backoff_time}s before retry (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(backoff_time)
                    continue
                else:
                    print(f"Rate limit exceeded, max retries reached")
                    return None
            elif res.status >= 500:
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


def get_user_following_ids(username, max_following=10000):
    """Get list of user IDs that a user is following using RapidAPI

    Args:
        username: The username to fetch followings for
        max_following: Maximum number of following IDs to fetch

    Returns:
        Set of following user IDs
    """
    following_ids_set = set()
    cursor = None
    page = 0
    max_pages = 100

    while page < max_pages:
        page += 1

        params = f"username={username}&count=500"
        if cursor:
            params += f"&cursor={cursor}"

        response = make_request("/following-ids", params)

        if not response:
            break

        try:
            new_ids = response.get("ids", [])

            if not new_ids:
                break

            # Convert all IDs to strings for consistency
            following_ids_set.update(str(uid) for uid in new_ids)

            next_cursor = response.get("next_cursor")
            if next_cursor and next_cursor != cursor and next_cursor != 0:
                cursor = next_cursor
            else:
                break

            if len(following_ids_set) >= max_following:
                break

        except Exception as e:
            print(f"  Error parsing following IDs for @{username}: {e}")
            break

    return following_ids_set


def load_followings_file(raw_data_dir, seed_graph_name):
    """Load the followings JSON file

    Args:
        raw_data_dir: Directory containing raw data files
        seed_graph_name: Name of the seed graph

    Returns:
        Tuple of (data dict, master_list_ids set)
    """
    filename = os.path.join(raw_data_dir, f"{seed_graph_name}_followings.json")

    if not os.path.exists(filename):
        print(f"Error: {filename} not found")
        print("Please run fetch_followings.py first to create the followings file")
        return None, None

    print(f"Loading {filename}...")

    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    master_list = data.get("master_list", [])
    master_list_ids = set(
        str(user.get("user_id")) for user in master_list if user.get("user_id")
    )

    print(f"  Loaded {len(master_list_ids)} users in master list")

    return data, master_list_ids


def load_progress(raw_data_dir, seed_graph_name):
    """Load progress from existing extended followings file if it exists

    Returns:
        Tuple of (processed_user_ids set, existing_data dict or None)
    """
    filename = os.path.join(raw_data_dir, f"{seed_graph_name}_extended_followings.json")

    if not os.path.exists(filename):
        return set(), None

    try:
        print(f"Loading existing progress from {filename}...")
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)

        users = data.get("users", [])
        processed_ids = set(
            str(user.get("user_id")) for user in users if user.get("user_id")
        )
        print(f"  Found {len(processed_ids)} already processed users")

        return processed_ids, data

    except Exception as e:
        print(f"  Could not load progress file: {e}")
        return set(), None


def save_extended_followings(output_data, raw_data_dir, seed_graph_name):
    """Save extended followings to JSON file"""
    filename = os.path.join(raw_data_dir, f"{seed_graph_name}_extended_followings.json")
    os.makedirs(raw_data_dir, exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"✓ Saved extended followings to: {filename}")


def fetch_single_user_followings(user, master_list_ids, index, total_users):
    """Fetch followings for a single user

    Args:
        user: User dict with user_id, username, display_name
        master_list_ids: Set of user IDs in master list
        index: Current index for logging
        total_users: Total number of users for logging

    Returns:
        User data dict or None if failed
    """
    user_id = str(user.get("user_id", ""))
    username = user.get("username", "unknown")

    if not user_id:
        return None

    print(
        f"[{index}/{total_users}] Fetching followings for @{username} (ID: {user_id})..."
    )

    try:
        # Fetch all following IDs for this user (using username)
        following_ids = get_user_following_ids(username)

        # Filter to only include IDs that are in master_list
        filtered_following_ids = following_ids.intersection(master_list_ids)

        # Remove self-follows
        filtered_following_ids.discard(user_id)

        user_data = {
            "user_id": user_id,
            "username": username,
            "display_name": user.get("display_name", username),
            "total_followings": len(following_ids),
            "filtered_followings_count": len(filtered_following_ids),
            "following_ids": list(filtered_following_ids),
        }

        print(
            f"  ✓ @{username}: {len(following_ids)} total, {len(filtered_following_ids)} in master list"
        )

        return user_data

    except Exception as e:
        print(f"  ✗ Error fetching @{username}: {e}")
        return None


def fetch_extended_followings(
    master_list, master_list_ids, processed_ids, max_parallel=4, save_interval=100
):
    """Fetch followings for all users in master list, filtered to master list only

    Args:
        master_list: List of user dicts from followings file
        master_list_ids: Set of user IDs in master list
        processed_ids: Set of already processed user IDs
        max_parallel: Maximum number of parallel requests
        save_interval: How often to save progress

    Returns:
        Generator yielding (users_data, total_filtered_followings) tuples
    """
    users_data = []
    total_filtered_followings = 0

    # Sort by user_id for consistent ordering
    sorted_users = sorted(master_list, key=lambda x: int(x.get("user_id", 0)))

    # Filter out already processed users
    users_to_process = []
    for i, user in enumerate(sorted_users):
        user_id = str(user.get("user_id", ""))
        if user_id and user_id not in processed_ids:
            users_to_process.append((i + 1, user))  # Keep original index for logging

    total_users = len(sorted_users)
    remaining_users = len(users_to_process)

    print(f"Processing {remaining_users} users with {max_parallel} parallel workers...")

    # Process in batches for saving
    batch_size = save_interval

    for batch_start in range(0, remaining_users, batch_size):
        batch_end = min(batch_start + batch_size, remaining_users)
        batch = users_to_process[batch_start:batch_end]

        # Process batch in parallel
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {
                executor.submit(
                    fetch_single_user_followings,
                    user,
                    master_list_ids,
                    idx,
                    total_users,
                ): (idx, user)
                for idx, user in batch
            }

            for future in as_completed(futures):
                idx, user = futures[future]
                try:
                    result = future.result()
                    if result:
                        users_data.append(result)
                        total_filtered_followings += result.get(
                            "filtered_followings_count", 0
                        )
                except Exception as e:
                    print(
                        f"  ✗ Exception for user {user.get('username', 'unknown')}: {e}"
                    )

        # Yield after each batch for saving
        yield users_data, total_filtered_followings


def main():
    """Main function - fetch extended followings for master list users"""
    global rate_limiter, request_count, start_time

    try:
        # Load configuration
        config = load_config()
        if not config:
            return

        # Get seed graph name from config
        seed_graph_config = config.get("seed_graph", {})
        if not seed_graph_config:
            print("Error: No [seed_graph] section found in config.toml")
            return

        seed_graph_name = (
            list(seed_graph_config.keys())[0] if seed_graph_config else "unknown"
        )
        print(f"Using seed_graph: {seed_graph_name}")

        # Get raw_data_dir
        script_dir = os.path.dirname(os.path.abspath(__file__))
        raw_data_dir_config = config.get("output", {}).get("raw_data_dir", "./raw")
        raw_data_dir = os.path.join(script_dir, raw_data_dir_config.lstrip("./"))

        # Load followings file
        followings_data, master_list_ids = load_followings_file(
            raw_data_dir, seed_graph_name
        )
        if not followings_data or not master_list_ids:
            return

        master_list = followings_data.get("master_list", [])

        # Load existing progress
        processed_ids, existing_data = load_progress(raw_data_dir, seed_graph_name)

        # Initialize rate limiter
        requests_per_second = config.get("rate_limiting", {}).get(
            "requests_per_second", 10
        )
        rate_limiter = RateLimiter(requests_per_second)

        # Get max parallel requests from config
        max_parallel = config.get("rate_limiting", {}).get("max_parallel_requests", 4)

        # Reset counters
        request_count = 0
        start_time = None

        print(f"\n{'=' * 60}")
        print(f"Extended Followings Fetcher")
        print(f"{'=' * 60}")
        print(f"Seed graph: {seed_graph_name}")
        print(f"Master list size: {len(master_list_ids)} users")
        print(f"Already processed: {len(processed_ids)} users")
        print(f"Remaining: {len(master_list_ids) - len(processed_ids)} users")
        print(f"Rate limiting: {requests_per_second} requests/second")
        print(f"Max parallel requests: {max_parallel}")
        print(f"{'=' * 60}\n")

        # Prepare output data structure
        if existing_data:
            output_data = existing_data
            users_data = existing_data.get("users", [])
        else:
            output_data = {
                "timestamp": datetime.now().isoformat(),
                "seed_graph": seed_graph_name,
                "master_list_size": len(master_list_ids),
                "users": [],
            }
            users_data = []

        save_interval = 1000  # Save every 1000 users

        # Fetch extended followings
        for batch_users, total_filtered in fetch_extended_followings(
            master_list, master_list_ids, processed_ids, max_parallel, save_interval
        ):
            # Update with all collected data
            output_data["users"] = batch_users
            output_data["timestamp"] = datetime.now().isoformat()
            output_data["total_users_processed"] = len(batch_users)
            output_data["total_filtered_followings"] = total_filtered

            # Save progress
            save_extended_followings(output_data, raw_data_dir, seed_graph_name)
            print(f"\n  Progress saved: {len(batch_users)} users processed\n")

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            final_users = output_data.get("users", [])
            total_filtered = sum(
                u.get("filtered_followings_count", 0) for u in final_users
            )

            print(f"\n{'=' * 60}")
            print(f"EXTENDED FOLLOWINGS FETCH COMPLETE")
            print(f"{'=' * 60}")
            print(f"Total users processed: {len(final_users)}")
            print(f"Total filtered followings: {total_filtered}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Progress has been saved.")
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

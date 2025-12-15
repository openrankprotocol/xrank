#!/usr/bin/env python3
"""
Extended Followings Fetcher for Seed Graph

This script extends the seed followings network by:
1. Loading raw/{seed_user_id}_seed_followings.json to get the master list
2. Fetching followings for all users in master list (excluding seed users - we already have their followings)
3. Saving extended followings to raw/{seed_user_id}_seed_extended_followings.json

Uses endpoints:
- /following-ids to get following IDs

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


def load_seed_followings(raw_data_dir):
    """Load the seed followings master list for the active seed user from config"""
    # Load config to get active seed user ID
    config = load_config()
    if not config:
        return None, None, None

    # Get active seed user IDs from [seed_graph] section
    seed_graph_config = config.get("seed_graph", {})
    seed_user_ids = []
    for community_name, user_ids in seed_graph_config.items():
        if isinstance(user_ids, list):
            seed_user_ids.extend(str(uid) for uid in user_ids)
    seed_user_ids = [uid for uid in seed_user_ids if uid and uid.isdigit()]

    if not seed_user_ids:
        print("Error: No active seed user ID found in config.toml [seed_graph] section")
        print("Please add user IDs to a community in config.toml")
        return None, None, None

    active_user_id = seed_user_ids[0]
    print(f"Using active seed user ID: {active_user_id}")

    # Try to find the seed_followings file by searching for files with this user ID
    # Files are named {user_id}_seed_followings.json
    import glob

    pattern = os.path.join(raw_data_dir, "*_seed_followings.json")
    matching_files = glob.glob(pattern)

    filename = None
    for file_path in matching_files:
        # Extract user_id from filename
        basename = os.path.basename(file_path)
        file_user_id = basename.split("_seed_followings.json")[0]
        if file_user_id == active_user_id:
            filename = file_path
            break

    if not filename:
        print(f"Error: Seed followings file not found for user ID {active_user_id}")
        print(
            f"Please run fetch_followings.py first with user ID {active_user_id} in config.toml"
        )
        return None, None, None

    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)

        master_list = data.get("master_list", [])
        seed_users = data.get("seed_users", [])

        # Extract seed_user_id from filename
        seed_user_id = os.path.basename(filename).split("_seed_followings.json")[0]

        print(f"Loaded seed followings from {filename}")
        print(f"  - Seed user ID: {seed_user_id}")
        print(f"  - Seed users: {len(seed_users)}")
        print(f"  - Total users in master list: {len(master_list)}")

        return master_list, seed_users, seed_user_id

    except Exception as e:
        print(f"Error loading seed followings: {e}")
        return None, None, None


def get_user_followings(user, max_following=1000):
    """Get list of user IDs that a user is following using RapidAPI"""
    username = user.get("username", "")
    user_id = user.get("user_id", "")

    # If username is empty, we can't fetch followings
    if not username:
        print(f"  Skipping user with ID {user_id} (no username)")
        return None

    print(f"  Fetching followings for @{username}")

    following_ids_set = set()  # Track unique IDs
    cursor = None
    page = 0
    max_pages = 10  # Limit pages per user

    while page < max_pages:
        page += 1

        # RapidAPI uses username and count parameters (max 5000)
        params = f"username={username}&count=5000"
        if cursor:
            params += f"&cursor={cursor}"

        response = make_request("/following-ids", params)

        if not response:
            print(f"    Page {page}: No response received")
            break

        try:
            # Extract following IDs from response - ids are at root level
            new_ids = response.get("ids", [])

            if not new_ids:
                print(f"    Page {page}: No following IDs in response")
                break

            before_count = len(following_ids_set)
            following_ids_set.update(new_ids)
            after_count = len(following_ids_set)
            new_count = after_count - before_count

            print(
                f"    Page {page}: Got {len(new_ids)} IDs, {new_count} new unique (total: {after_count})"
            )

            # Check for next page
            next_cursor = response.get("next_cursor")
            if next_cursor and next_cursor != cursor:
                cursor = next_cursor
            else:
                print(f"    No more pages")
                break

            if len(following_ids_set) >= max_following:
                print(f"    Reached ID limit of {max_following}")
                break

        except Exception as e:
            print(f"    Page {page}: Error parsing following IDs: {e}")
            break

    following_ids_list = list(following_ids_set)
    print(f"  ✓ Found {len(following_ids_list)} following IDs for @{username}")

    return {
        "user_id": user_id,
        "username": username,
        "following_ids": following_ids_list,
        "following_count": len(following_ids_list),
    }


def load_checkpoint(raw_data_dir, seed_user_id):
    """Load checkpoint if exists"""
    checkpoint_file = os.path.join(
        raw_data_dir, f"{seed_user_id}_seed_extended_followings_checkpoint.json"
    )

    if not os.path.exists(checkpoint_file):
        return []

    try:
        with open(checkpoint_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"Loaded checkpoint with {len(data)} users processed")
        return data
    except Exception as e:
        print(f"Warning: Could not load checkpoint: {e}")
        return []


def save_checkpoint(extended_followings, raw_data_dir, seed_user_id):
    """Save checkpoint to allow resuming"""
    checkpoint_file = os.path.join(
        raw_data_dir, f"{seed_user_id}_seed_extended_followings_checkpoint.json"
    )

    try:
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(extended_followings, f, indent=2, ensure_ascii=False)
        print(f"  Checkpoint saved: {len(extended_followings)} users processed")
    except Exception as e:
        print(f"  Warning: Could not save checkpoint: {e}")


def cleanup_checkpoint(raw_data_dir, seed_user_id):
    """Remove checkpoint file after successful completion"""
    checkpoint_file = os.path.join(
        raw_data_dir, f"{seed_user_id}_seed_extended_followings_checkpoint.json"
    )

    try:
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            print(f"Checkpoint file removed")
    except Exception as e:
        print(f"Warning: Could not remove checkpoint file: {e}")


def save_extended_followings(extended_followings, raw_data_dir, seed_user_id):
    """Save extended followings to JSON file"""
    filename = os.path.join(
        raw_data_dir, f"{seed_user_id}_seed_extended_followings.json"
    )
    os.makedirs(raw_data_dir, exist_ok=True)

    output_data = {
        "timestamp": datetime.now().isoformat(),
        "total_users": len(extended_followings),
        "users": extended_followings,
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Saved extended followings to: {filename}")
    print(f"  - Total users: {len(extended_followings)}")

    # Calculate stats
    total_followings = sum(len(u.get("following_ids", [])) for u in extended_followings)
    print(f"  - Total followings collected: {total_followings}")

    return filename


def main():
    """Main function - fetch extended followings for master list users"""
    global rate_limiter, request_count, start_time

    # Initialize to avoid unbound variable in exception handler
    raw_data_dir = "./raw/seed"

    try:
        # Load configuration
        config = load_config()
        if not config:
            return

        # Initialize rate limiter with config values (default to 10 for RapidAPI)
        requests_per_second = config.get("rate_limiting", {}).get(
            "requests_per_second", 10
        )
        rate_limiter = RateLimiter(requests_per_second)

        # Get max_parallel parameter (default to 4 if not in config)
        max_parallel = config.get("rate_limiting", {}).get("max_parallel_requests", 4)

        # Reset counters
        request_count = 0
        start_time = None

        print(f"Extended Followings Fetcher")
        print(f"Rate limiting: {requests_per_second} requests/second")
        print(f"Max parallel requests: {max_parallel}")
        print(f"=" * 60)

        # Get raw_data_dir and make it relative to project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.join(script_dir, "..")
        raw_data_dir_config = config.get("output", {}).get("raw_data_dir", "./raw")
        raw_data_dir = os.path.join(
            project_root, raw_data_dir_config.lstrip("./"), "seed"
        )

        max_following_per_user = config.get("data", {}).get(
            "max_following_per_user", 1000
        )

        print(f"Configuration:")
        print(f"  - Max following per user: {max_following_per_user}")
        print(f"  - Raw data directory: {raw_data_dir}")
        print(f"  - Parallel workers: {max_parallel}")

        # Load seed followings master list
        master_list, seed_users, seed_user_id = load_seed_followings(raw_data_dir)
        if not master_list or not seed_users or not seed_user_id:
            print("Error: Could not load seed followings")
            return

        # Get seed user IDs to exclude them
        seed_user_ids = {user.get("user_id") for user in seed_users}
        print(
            f"\nExcluding {len(seed_user_ids)} seed users (already have their followings)"
        )

        # Filter to only non-seed users
        non_seed_users = [
            u for u in master_list if u.get("user_id") not in seed_user_ids
        ]
        print(f"Total non-seed users to process: {len(non_seed_users)}")

        # Load checkpoint if exists
        processed_data = load_checkpoint(raw_data_dir, seed_user_id)
        processed_user_ids = {u.get("user_id") for u in processed_data}

        # Filter out already processed users
        remaining_users = [
            u for u in non_seed_users if u.get("user_id") not in processed_user_ids
        ]

        print(f"\nProgress:")
        print(f"  - Already processed: {len(processed_data)}")
        print(f"  - Remaining to process: {len(remaining_users)}")

        # Start with existing processed data
        extended_followings = processed_data.copy()

        # Process remaining users in parallel batches
        batch_size = max_parallel
        for batch_start in range(0, len(remaining_users), batch_size):
            batch_end = min(batch_start + batch_size, len(remaining_users))
            batch_users = remaining_users[batch_start:batch_end]

            print(
                f"\nProcessing batch {batch_start // batch_size + 1}: users {batch_start + 1}-{batch_end} of {len(remaining_users)}"
            )

            # Process batch in parallel
            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                # Submit all users in batch
                future_to_user = {
                    executor.submit(
                        get_user_followings, user, max_following_per_user
                    ): user
                    for user in batch_users
                }

                # Collect results as they complete
                for future in as_completed(future_to_user):
                    user = future_to_user[future]
                    try:
                        user_followings = future.result()
                        if user_followings:
                            extended_followings.append(user_followings)
                            print(
                                f"  ✓ Completed @{user.get('username', 'unknown')} - {len(user_followings.get('following_ids', []))} followings"
                            )
                        else:
                            print(
                                f"  ⚠️  No followings data for @{user.get('username', 'unknown')}"
                            )
                    except Exception as e:
                        print(
                            f"  ✗ Error processing user @{user.get('username', 'unknown')}: {e}"
                        )
                        # Continue with other users even if one fails
                        continue

            # Save checkpoint after each batch
            save_checkpoint(extended_followings, raw_data_dir, seed_user_id)
            print(
                f"Progress: {len(extended_followings)}/{len(non_seed_users)} users processed"
            )

        # Save final results
        if extended_followings:
            save_extended_followings(extended_followings, raw_data_dir, seed_user_id)
            # Clean up checkpoint file after successful completion
            cleanup_checkpoint(raw_data_dir, seed_user_id)
        else:
            print("No extended followings data collected")

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'=' * 60}")
            print(f"EXTENDED FOLLOWINGS FETCH COMPLETE")
            print(f"{'=' * 60}")
            print(f"Users processed: {len(extended_followings)}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

        print(f"\nOutput file saved in {raw_data_dir}:")
        print(f"- {seed_user_id}_seed_extended_followings.json")

    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"\nCheckpoint file is preserved in {raw_data_dir} for resuming:")
        print(f"- seed_extended_followings_checkpoint.json")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

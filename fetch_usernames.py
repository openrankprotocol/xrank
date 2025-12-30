#!/usr/bin/env python3
"""
Username Fetcher for Scored Users

This script fetches usernames for all user IDs in the scores CSV:
1. Loads scores/[seed_graph].csv
2. Goes through all user IDs and fetches their usernames using /get-users-v2 API
3. Uses parallel processing based on max_parallel_requests config
4. Saves to raw/[seed_graph]_usernames.csv

Uses endpoints:
- /get-users-v2 to get user information for multiple user IDs (batch of 50)

Rate limited to comply with API limits.
"""

import csv
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


def fetch_single_batch(batch, batch_num, total_batches):
    """Fetch user info for a single batch of user IDs

    Args:
        batch: List of user IDs to fetch
        batch_num: Current batch number
        total_batches: Total number of batches

    Returns:
        List of (user_id, username) tuples
    """
    batch_users = []

    # Join user IDs with commas
    users_param = ",".join(str(uid) for uid in batch)
    params = f"users={users_param}"

    print(f"  Batch {batch_num}/{total_batches}: Fetching {len(batch)} users...")

    response = make_request("/get-users-v2", params)

    if not response:
        print(f"  Batch {batch_num}: No response")
        return batch_users

    # Response format: {"result": [...]}
    result = response.get("result", [])

    if not result:
        return batch_users

    # Process each user in the batch
    for user_data in result:
        try:
            user_id = str(user_data.get("id_str") or user_data.get("id", ""))
            username = user_data.get("screen_name", "")

            if user_id and username:
                batch_users.append((user_id, username))
        except Exception as e:
            continue

    return batch_users


def load_scores_file(scores_dir, seed_graph_name):
    """Load user IDs from scores CSV file

    Args:
        scores_dir: Directory containing scores files
        seed_graph_name: Name of the seed graph

    Returns:
        List of user IDs
    """
    filename = os.path.join(scores_dir, f"{seed_graph_name}.csv")

    if not os.path.exists(filename):
        print(f"Error: {filename} not found")
        return None

    print(f"Loading {filename}...")

    user_ids = []
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row.get("i", "").strip()
            if user_id:
                user_ids.append(user_id)

    print(f"  Loaded {len(user_ids)} user IDs from scores")
    return user_ids


def load_existing_usernames(raw_data_dir, seed_graph_name):
    """Load existing username mappings if file exists

    Returns:
        Dict mapping user_id -> username
    """
    filename = os.path.join(raw_data_dir, f"{seed_graph_name}_usernames.csv")

    if not os.path.exists(filename):
        return {}

    print(f"Loading existing usernames from {filename}...")

    username_map = {}
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row.get("user_id", "").strip()
            username = row.get("username", "").strip()
            if user_id and username:
                username_map[user_id] = username

    print(f"  Loaded {len(username_map)} existing usernames")
    return username_map


def save_usernames(username_map, raw_data_dir, seed_graph_name):
    """Save username mappings to CSV file"""
    filename = os.path.join(raw_data_dir, f"{seed_graph_name}_usernames.csv")
    os.makedirs(raw_data_dir, exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        f.write("username,user_id\n")
        for user_id, username in sorted(
            username_map.items(), key=lambda x: x[1].lower()
        ):
            f.write(f"{username},{user_id}\n")

    print(f"✓ Saved {len(username_map)} usernames to: {filename}")


def main():
    """Main function - fetch usernames for all scored users"""
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

        # Get directories
        script_dir = os.path.dirname(os.path.abspath(__file__))
        raw_data_dir_config = config.get("output", {}).get("raw_data_dir", "./raw")
        raw_data_dir = os.path.join(script_dir, raw_data_dir_config.lstrip("./"))
        scores_dir = os.path.join(script_dir, "scores")

        # Load user IDs from scores file
        user_ids = load_scores_file(scores_dir, seed_graph_name)
        if not user_ids:
            return

        # Start fresh - no loading existing usernames
        username_map = {}
        user_ids_to_fetch = user_ids
        print(f"Users to fetch: {len(user_ids_to_fetch)}")

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
        print(f"Username Fetcher")
        print(f"{'=' * 60}")
        print(f"Seed graph: {seed_graph_name}")
        print(f"Total users to fetch: {len(user_ids_to_fetch)}")
        print(f"Rate limiting: {requests_per_second} requests/second")
        print(f"Max parallel requests: {max_parallel}")
        print(f"{'=' * 60}\n")

        # Create batches of 100 users
        batch_size = 100
        total_batches = (len(user_ids_to_fetch) + batch_size - 1) // batch_size

        batches = []
        for i in range(0, len(user_ids_to_fetch), batch_size):
            batch = user_ids_to_fetch[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            batches.append((batch, batch_num, total_batches))

        print(
            f"Processing {len(user_ids_to_fetch)} users in {total_batches} batches..."
        )

        # Process batches in parallel with periodic saves
        save_interval = 100  # Save every 100 batches
        processed_batches = 0

        for chunk_start in range(0, len(batches), save_interval):
            chunk_end = min(chunk_start + save_interval, len(batches))
            chunk = batches[chunk_start:chunk_end]

            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                future_to_batch = {
                    executor.submit(
                        fetch_single_batch, batch, batch_num, total_batches
                    ): batch_num
                    for batch, batch_num, total_batches in chunk
                }

                for future in as_completed(future_to_batch):
                    batch_num = future_to_batch[future]
                    try:
                        batch_users = future.result()
                        for user_id, username in batch_users:
                            username_map[user_id] = username
                        processed_batches += 1
                    except Exception as e:
                        print(f"  Batch {batch_num}: Exception occurred: {e}")
                        continue

            print(
                f"  ✓ Processed {processed_batches}/{total_batches} batches, {len(username_map)} usernames collected"
            )

            # Save progress after each chunk
            save_usernames(username_map, raw_data_dir, seed_graph_name)

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0

            print(f"\n{'=' * 60}")
            print(f"USERNAME FETCH COMPLETE")
            print(f"{'=' * 60}")
            print(f"Total usernames collected: {len(username_map)}")
            print(f"Users without usernames: {len(user_ids) - len(username_map)}")
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

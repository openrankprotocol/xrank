#!/usr/bin/env python3
"""
Seed Users Following Fetcher

This script fetches followings of seed users and creates a master list:
1. Loads seed usernames from config.toml [seed_users] section
2. For each seed user, fetches all their followings
3. Creates a master list containing:
   - Seed users themselves
   - All users they follow (deduplicated)
4. Saves to raw/seed_followings.json

Uses endpoints:
- /twitter/user/followings to get following users

Rate limited to 1,000 requests per second to comply with API limits.
"""

import http.client
import json
import os
import threading
import time
from datetime import datetime

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


def get_user_followings(username, max_following=10000):
    """Get list of users that a user is following using new API"""
    print(f"\nFetching followings for @{username}")

    following_users = []  # List of user info dicts
    following_ids_set = set()  # Track unique IDs
    cursor = None
    page = 0
    max_pages = 100  # Reasonable limit

    while page < max_pages:
        page += 1

        # New API uses userName parameter with pageSize (max 200)
        params = {"userName": username, "pageSize": "200"}
        if cursor:
            params["cursor"] = cursor

        print(f"  Page {page}: Fetching...")

        response = make_request("/twitter/user/followings", params)

        if not response:
            print(f"  Page {page}: No response received")
            break

        # Check API response status
        if response.get("status") != "success":
            print(f"  API error: {response.get('message', 'Unknown error')}")
            break

        try:
            # Extract following users from API format
            followings = response.get("followings", [])

            if not followings:
                print(f"  Page {page}: No followings in response")
                break

            # Process each following user
            new_count = 0
            for following in followings:
                user_id = following.get("id")
                if user_id and user_id not in following_ids_set:
                    following_ids_set.add(user_id)
                    following_users.append(
                        {
                            "user_id": user_id,
                            "username": following.get("userName", ""),
                            "display_name": following.get("name", ""),
                            "followers_count": following.get("followers", 0),
                            "following_count": following.get("following", 0),
                            "verified": following.get("isBlueVerified", False),
                            "verified_type": following.get("verifiedType", ""),
                            "description": following.get("description", ""),
                            "location": following.get("location", ""),
                            "created_at": following.get("createdAt", ""),
                        }
                    )
                    new_count += 1

            print(
                f"  Page {page}: Got {len(followings)} users, {new_count} new unique (total: {len(following_users)})"
            )

            # Check for next page
            if response.get("has_next_page"):
                cursor = response.get("next_cursor")
            else:
                print(f"  No more pages")
                break

            if not cursor:
                break

            if len(following_users) >= max_following:
                print(f"  Reached limit of {max_following} followings")
                break

        except Exception as e:
            print(f"  Page {page}: Error parsing followings for @{username}: {e}")
            import traceback

            traceback.print_exc()
            break

    print(f"  ✓ Found {len(following_users)} total followings for @{username}")
    return following_users


def build_master_list(seed_usernames):
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
            following_users = get_user_followings(username)

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
                # Seed user not in their own followings, add manually with minimal info
                seed_user_id = f"seed_{username}"  # Temporary ID
                seed_user = {
                    "user_id": seed_user_id,
                    "username": username,
                    "display_name": username,
                    "type": "seed",
                    "followers_count": 0,
                    "following_count": len(following_users),
                    "verified": False,
                    "verified_type": "",
                    "description": "",
                    "location": "",
                    "created_at": "",
                }
                master_dict[seed_user_id] = seed_user
                seed_users_info.append(seed_user)

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
    filename = os.path.join(raw_data_dir, "seed_followings.json")
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
    raw_data_dir = "./raw"

    try:
        # Load configuration
        config = load_config()
        if not config:
            return

        # Get seed usernames from config
        seed_usernames = config.get("seed_users", {}).get("usernames", [])
        if not seed_usernames:
            print("Error: No seed usernames found in config.toml [seed_users] section")
            return

        # Filter out commented usernames (shouldn't happen with TOML, but just in case)
        seed_usernames = [u for u in seed_usernames if u and not u.startswith("#")]

        if not seed_usernames:
            print("Error: No valid seed usernames found")
            return

        # Initialize rate limiter with config values
        requests_per_second = config.get("rate_limiting", {}).get(
            "requests_per_second", 1000
        )
        rate_limiter = RateLimiter(requests_per_second)

        # Reset counters
        request_count = 0
        start_time = None

        print(f"Seed Users Following Fetcher")
        print(f"Rate limiting: {requests_per_second} requests/second")
        print(f"Seed users to process: {len(seed_usernames)}")
        print(f"=" * 60)

        raw_data_dir = config.get("output", {}).get("raw_data_dir", "./raw")

        # Build master list from seed users' followings
        master_list, seed_users_info = build_master_list(seed_usernames)

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

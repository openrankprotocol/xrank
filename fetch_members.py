#!/usr/bin/env python3
"""
Community Members Fetcher

This script fetches community members from Twitter/X communities:
1. Uses /twitter/community/members endpoint to get all members (includes moderators)
2. Saves results to raw/{community_id}_members.json

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

    def __init__(self, requests_per_second=1000, burst_size=10):
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
            "rate_limiting": {"request_delay": 1.0, "community_delay": 2.0},
        }


def get_api_key():
    """Get API key from .env file or environment, removing any quotes"""
    api_key = os.getenv("TWITTER_API_KEY")

    if not api_key:
        try:
            with open(".env", "r") as f:
                for line in f:
                    if line.startswith("TWITTER_API_KEY="):
                        api_key = line.split("=", 1)[1].strip()
                        break
        except FileNotFoundError:
            pass

    if not api_key:
        raise ValueError(
            "TWITTER_API_KEY not found in environment variables or .env file"
        )

    # Remove any surrounding quotes that cause authentication to fail
    api_key = api_key.strip("\"'")

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


def get_community_members(community_id):
    """Get all members of a community using new API"""
    all_members = []

    try:
        print(f"Fetching members for community: {community_id}")
        cursor = None
        page = 0

        while True:
            page += 1
            print(f"Fetching members page {page}...")

            params = {"community_id": community_id}
            if cursor:
                params["cursor"] = cursor

            response = make_request("/twitter/community/members", params)

            if not response:
                break

            # Check API response status
            if response.get("status") != "success":
                print(f"API error: {response.get('msg', 'Unknown error')}")
                break

            # Parse new API response structure
            members = response.get("members", [])

            if not members:
                break

            for member in members:
                # Extract member info from new API format
                member_info = {
                    "username": member.get("userName", ""),
                    "name": member.get("name", ""),
                    "id": member.get("id", ""),
                    "followers_count": member.get("followers", 0),
                    "following_count": member.get("following", 0),
                    "verified": member.get("isBlueVerified", False),
                    "verified_type": member.get("verifiedType", ""),
                    "description": member.get("description", ""),
                    "profile_image_url": member.get("profilePicture", ""),
                    "location": member.get("location", ""),
                    "created_at": member.get("createdAt", ""),
                    "statuses_count": member.get("statusesCount", 0),
                }

                all_members.append(member_info)

            print(
                f"Found {len(members)} users on page {page} (total: {len(all_members)})"
            )

            # Check for next page
            if response.get("has_next_page"):
                cursor = response.get("next_cursor")
            else:
                cursor = None

            if not cursor:
                break

        print(f"Total members fetched: {len(all_members)}")

    except Exception as e:
        print(f"Error fetching members for {community_id}: {e}")
        import traceback

        traceback.print_exc()

    return all_members


def save_members_to_file(all_members, community_id, raw_data_dir):
    """Save community members list to JSON file"""
    # Save member data for identification purposes
    member_list = {
        "community_id": community_id,
        "timestamp": datetime.now().isoformat(),
        "members": [
            {
                "username": member.get("username"),
                "display_name": member.get("name"),
                "user_id": member.get("id"),
                "role": "Member",  # New API doesn't distinguish roles in the response
            }
            for member in all_members
            if member.get("username")
        ],
        "moderators": [],  # Empty for backwards compatibility
    }

    filename = os.path.join(raw_data_dir, f"{community_id}_members.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(member_list, f, indent=2, ensure_ascii=False)

    total_members = len(member_list["members"])
    print(f"Saved {total_members} members to: {filename}")
    return filename


def main():
    """Main function - fetch community members and save to JSON files"""
    global rate_limiter, request_count, start_time

    # Initialize to avoid unbound variable in exception handler
    raw_data_dir = "./raw"

    try:
        # Load configuration
        config = load_config()

        # Initialize rate limiter with config values
        requests_per_second = config["rate_limiting"]["requests_per_second"]
        rate_limiter = RateLimiter(requests_per_second)

        # Reset counters
        request_count = 0
        start_time = None

        print(f"Community Members Fetcher")
        print(f"Rate limiting: {requests_per_second} requests/second")
        print(f"=" * 50)

        community_ids = config["communities"]["ids"]
        raw_data_dir = config["output"]["raw_data_dir"]
        community_delay = config["rate_limiting"]["community_delay"]

        for i, community_id in enumerate(community_ids):
            print(f"\n{'=' * 50}")
            print(f"Processing community {i + 1}/{len(community_ids)}: {community_id}")
            print(f"{'=' * 50}")

            # Fetch community members
            print(f"Fetching community members...")
            all_members = get_community_members(community_id)
            save_members_to_file(all_members, community_id, raw_data_dir)

            # Delay between communities
            if i < len(community_ids) - 1:
                print(f"Waiting {community_delay} seconds before next community...")
                time.sleep(community_delay)

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'=' * 50}")
            print(f"MEMBERS FETCH COMPLETE")
            print(f"{'=' * 50}")
            print(f"Communities processed: {len(community_ids)}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

        print(f"\nOutput files saved in {raw_data_dir}:")
        print(f"- [community_id]_members.json")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

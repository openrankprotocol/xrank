#!/usr/bin/env python3
"""
Community Members Fetcher

This script fetches community members and moderators from Twitter/X communities:
1. Uses /community-members endpoint to get all members
2. Uses /community-moderators endpoint to get all moderators
3. Saves results to raw/{community_id}_members.json

Rate limited to 10 requests per second to comply with API limits.
"""

import http.client
import json
import os
import threading
import time
from datetime import datetime, timedelta

import toml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class RateLimiter:
    """Rate limiter to ensure we don't exceed API rate limits"""

    def __init__(self, requests_per_second=10, burst_size=10):
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
    api_key = os.getenv("RAPIDAPI_KEY")

    if not api_key:
        try:
            with open(".env", "r") as f:
                for line in f:
                    if line.startswith("RAPIDAPI_KEY="):
                        api_key = line.split("=", 1)[1].strip()
                        break
        except FileNotFoundError:
            pass

    if not api_key:
        raise ValueError("RAPIDAPI_KEY not found in environment variables or .env file")

    # CRITICAL FIX: Remove any surrounding quotes that cause authentication to fail
    api_key = api_key.strip("\"'")

    return api_key


def make_request(endpoint, params="", max_retries=3):
    """Make HTTP request to RapidAPI with rate limiting and exponential backoff"""
    global request_count, start_time

    # Initialize start time on first request
    if start_time is None:
        start_time = time.time()

    for attempt in range(max_retries):
        # Wait for rate limiter before making request
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


def get_community_details(community_id):
    """Get community metadata (name, description, created_at, etc.)"""
    print(f"Fetching community details for: {community_id}")

    params = f"communityId={community_id}"
    response = make_request("/community-details", params)

    if not response:
        print(f"Failed to fetch community details for {community_id}")
        return None

    # Parse the response structure
    result = response.get("result", {}).get("result", {})

    if not result:
        print(f"No community details found for {community_id}")
        return None

    # Extract creator info
    creator_info = None
    creator_results = result.get("creator_results", {}).get("result", {})
    if creator_results:
        creator_legacy = creator_results.get("legacy", {})
        creator_info = {
            "username": creator_legacy.get("screen_name"),
            "is_blue_verified": creator_results.get("is_blue_verified", False),
            "verified": creator_results.get("verification", {}).get("verified", False),
        }

    # Extract rules
    rules = []
    for rule in result.get("rules", []):
        rules.append({"id": rule.get("rest_id"), "name": rule.get("name")})

    # Convert created_at from milliseconds to ISO format
    created_at_ms = result.get("created_at")
    created_at = None
    if created_at_ms:
        created_at = datetime.fromtimestamp(created_at_ms / 1000).isoformat()

    details = {
        "community_id": community_id,
        "name": result.get("name"),
        "description": result.get("description"),
        "created_at": created_at,
        "created_at_timestamp": created_at_ms,
        "join_policy": result.get("join_policy"),
        "is_member": result.get("is_member", False),
        "creator": creator_info,
        "rules": rules,
        "member_count": result.get("member_count"),
        "moderator_count": result.get("moderator_count"),
    }

    print(f"Community: {details.get('name')} (created: {created_at})")

    return details


def get_community_members(community_id):
    """Get all members and moderators of a community"""
    members_data = {"members": [], "moderators": []}

    # Get community members
    try:
        print(f"Fetching members for community: {community_id}")
        cursor = None
        page = 0

        while True:
            page += 1
            print(f"Fetching members page {page}...")

            params = f"communityId={community_id}"
            if cursor:
                params += f"&cursor={cursor}"

            members_response = make_request("/community-members", params)

            if not members_response:
                break

            # Parse the correct response structure
            if (
                "result" in members_response
                and "members_slice" in members_response["result"]
            ):
                items_results = members_response["result"]["members_slice"].get(
                    "items_results", []
                )

                for item in items_results:
                    if "result" in item and "legacy" in item["result"]:
                        user = item["result"]
                        legacy = user["legacy"]

                        member_info = {
                            "username": legacy.get("screen_name", ""),
                            "name": legacy.get("name", ""),
                            "id": legacy.get("id_str", ""),
                            "followers_count": legacy.get("followers_count", 0),
                            "verified": user.get("verification", {}).get(
                                "verified", False
                            ),
                            "is_blue_verified": user.get("is_blue_verified", False),
                            "description": legacy.get("description", ""),
                            "profile_image_url": legacy.get(
                                "profile_image_url_https", ""
                            ),
                            "community_role": user.get("community_role", "Member"),
                            "protected": user.get("privacy", {}).get(
                                "protected", False
                            ),
                        }

                        # Separate members and moderators based on role
                        if user.get("community_role") == "Moderator":
                            members_data["moderators"].append(member_info)
                        else:
                            members_data["members"].append(member_info)

                # Check for next page cursor
                cursor = None
                if (
                    "cursor" in members_response
                    and "bottom" in members_response["cursor"]
                ):
                    cursor_data = members_response["cursor"]["bottom"]
                    if isinstance(cursor_data, dict) and "next_cursor" in cursor_data:
                        cursor = cursor_data["next_cursor"]
                elif (
                    "result" in members_response
                    and "members_slice" in members_response["result"]
                ):
                    slice_info = members_response["result"]["members_slice"].get(
                        "slice_info", {}
                    )
                    if "next_cursor" in slice_info:
                        cursor = slice_info["next_cursor"]

                if not cursor or not items_results:
                    break

                print(f"Found {len(items_results)} users on page {page}")
            else:
                break

        print(
            f"Found {len(members_data['members'])} members and {len(members_data['moderators'])} moderators"
        )

    except Exception as e:
        print(f"Error fetching members for {community_id}: {e}")

    # Also try community-moderators endpoint for additional moderators
    try:
        print(f"Fetching additional moderators for community: {community_id}")
        mod_cursor = None
        mod_page = 0

        while True:
            mod_page += 1
            print(f"Fetching moderators page {mod_page}...")

            mod_params = f"communityId={community_id}"
            if mod_cursor:
                mod_params += f"&cursor={mod_cursor}"

            moderators_response = make_request("/community-moderators", mod_params)

            if not moderators_response:
                break

            # Parse moderators response structure with moderators_slice
            if (
                "result" in moderators_response
                and "moderators_slice" in moderators_response["result"]
            ):
                items_results = moderators_response["result"]["moderators_slice"].get(
                    "items_results", []
                )

                for item in items_results:
                    if "result" in item and "legacy" in item["result"]:
                        user = item["result"]
                        legacy = user["legacy"]

                        moderator_info = {
                            "username": legacy.get("screen_name", ""),
                            "name": legacy.get("name", ""),
                            "id": legacy.get("id_str", ""),
                            "followers_count": legacy.get("followers_count", 0),
                            "verified": user.get("verification", {}).get(
                                "verified", False
                            ),
                            "is_blue_verified": user.get("is_blue_verified", False),
                            "description": legacy.get("description", ""),
                            "profile_image_url": legacy.get(
                                "profile_image_url_https", ""
                            ),
                            "community_role": user.get("community_role", "Moderator"),
                            "protected": user.get("privacy", {}).get(
                                "protected", False
                            ),
                        }

                        # Check if already added from members endpoint
                        existing_mod = next(
                            (
                                m
                                for m in members_data["moderators"]
                                if m["id"] == moderator_info["id"]
                            ),
                            None,
                        )
                        if not existing_mod:
                            members_data["moderators"].append(moderator_info)

                # Check for next page cursor for moderators
                mod_cursor = None
                if (
                    "cursor" in moderators_response
                    and "bottom" in moderators_response["cursor"]
                ):
                    cursor_data = moderators_response["cursor"]["bottom"]
                    if isinstance(cursor_data, dict) and "next_cursor" in cursor_data:
                        mod_cursor = cursor_data["next_cursor"]
                elif (
                    "result" in moderators_response
                    and "moderators_slice" in moderators_response["result"]
                ):
                    slice_info = moderators_response["result"]["moderators_slice"].get(
                        "slice_info", {}
                    )
                    if "next_cursor" in slice_info:
                        mod_cursor = slice_info["next_cursor"]

                if not mod_cursor or not items_results:
                    break

                print(f"Found {len(items_results)} moderators on page {mod_page}")
            else:
                break

    except Exception as e:
        print(f"Error fetching additional moderators for {community_id}: {e}")

    return members_data


def save_members_to_file(
    members_data, community_id, raw_data_dir, community_details=None
):
    """Save community members list to JSON file"""
    # Save member data for identification purposes
    member_list = {
        "community_id": community_id,
        "timestamp": datetime.now().isoformat(),
        "community_name": community_details.get("name") if community_details else None,
        "community_description": community_details.get("description")
        if community_details
        else None,
        "community_created_at": community_details.get("created_at")
        if community_details
        else None,
        "join_policy": community_details.get("join_policy")
        if community_details
        else None,
        "creator": community_details.get("creator") if community_details else None,
        "rules": community_details.get("rules") if community_details else None,
        "members": [
            {
                "username": member.get("username"),
                "display_name": member.get("name"),
                "user_id": member.get("id"),
            }
            for member in members_data.get("members", [])
            if member.get("username")
        ],
        "moderators": [
            {
                "username": mod.get("username"),
                "display_name": mod.get("name"),
                "user_id": mod.get("id"),
            }
            for mod in members_data.get("moderators", [])
            if mod.get("username")
        ],
    }

    filename = os.path.join(raw_data_dir, f"{community_id}_members.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(member_list, f, indent=2, ensure_ascii=False)

    total_members = len(member_list["members"])
    total_moderators = len(member_list["moderators"])
    print(
        f"Saved {total_members} members and {total_moderators} moderators to: {filename}"
    )
    return filename


def main():
    """Main function - fetch community members and save to JSON files"""
    global rate_limiter, request_count, start_time

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

            # Fetch community details (name, description, created_at, etc.)
            print(f"Fetching community details...")
            community_details = get_community_details(community_id)

            # Fetch community members and moderators
            print(f"Fetching community members and moderators...")
            members_data = get_community_members(community_id)
            save_members_to_file(
                members_data, community_id, raw_data_dir, community_details
            )

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


if __name__ == "__main__":
    main()

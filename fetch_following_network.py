#!/usr/bin/env python3
"""
Following Network Fetcher

This script analyzes the following network within a Twitter/X community by:
1. Loading raw/[community_id]_members.json into a members list
2. Reading all users that each member is following to create extended_members list
3. Creating master_list = members + extended_members
4. Checking what other members from master_list each user is following
5. Saving the following network to raw/[community_id]_following_network.json

Uses endpoints:
- /following-ids?username={username}&count=500 to get following IDs
- /get-users-v2?users={user_ids} to get user info by IDs

Rate limited to 10 requests per second to comply with API limits.
"""

import http.client
import json
import os
import toml
from datetime import datetime
from dotenv import load_dotenv
import time
import threading
import urllib.parse

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
        with open('config.toml', 'r') as f:
            return toml.load(f)
    except FileNotFoundError:
        print("config.toml not found, using default values")
        return {
            'data': {'days_back': 730, 'post_limit': 10000, 'extended_members_limit': 100000, 'max_following_per_user': 1000},
            'communities': {'ids': ["1601841656147345410"]},
            'output': {'raw_data_dir': "./raw"},
            'rate_limiting': {'requests_per_second': 10, 'request_delay': 0.1, 'community_delay': 2.0}
        }
    except Exception as e:
        print(f"Error loading config: {e}")
        return None

def get_api_key():
    """Get API key from environment with proper formatting"""
    api_key = os.getenv('RAPIDAPI_KEY')

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

def make_request(endpoint, params="", max_retries=3, username=None):
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
            print(f"API Requests: {request_count}, Rate: {rate:.2f}/sec, Elapsed: {elapsed:.1f}s")

        try:
            conn = http.client.HTTPSConnection("twitter241.p.rapidapi.com")

            headers = {
                'x-rapidapi-key': get_api_key(),
                'x-rapidapi-host': "twitter241.p.rapidapi.com"
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
                    backoff_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    print(f"Rate limit hit, waiting {backoff_time}s before retry (attempt {attempt + 1}/{max_retries})")
                    time.sleep(backoff_time)
                    continue
                else:
                    print(f"Rate limit exceeded, max retries reached")
                    return None
            elif res.status >= 500:  # Server errors
                if attempt < max_retries - 1:
                    backoff_time = 2 ** attempt
                    print(f"Server error {res.status}, retrying in {backoff_time}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(backoff_time)
                    continue
                else:
                    error_msg = data.decode('utf-8') if data else 'No response data'
                    print(f"Error: HTTP {res.status} - {error_msg}")
                    if 'following-ids' in endpoint and res.status == 404:
                        print(f"    Possible issues: username may not exist or endpoint may be incorrect")
                    return None
            else:
                print(f"Error: HTTP {res.status} - {data.decode('utf-8')}")
                return None

        except Exception as e:
            if attempt < max_retries - 1:
                backoff_time = 2 ** attempt
                print(f"Request failed: {str(e)}, retrying in {backoff_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(backoff_time)
                continue
            else:
                print(f"Request failed after {max_retries} attempts: {str(e)}")
                return None

    return None

def load_members_list(community_id, raw_data_dir):
    """Load the members list from fetch_members.py output"""
    filename = os.path.join(raw_data_dir, f"{community_id}_members.json")

    if not os.path.exists(filename):
        print(f"Error: Members file not found: {filename}")
        print("Please run fetch_members.py first to generate the members list.")
        return None

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            members_data = json.load(f)

        # Extract all members and moderators
        members = []

        # Add regular members
        for member in members_data.get('members', []):
            if member.get('username') and member.get('user_id'):
                members.append({
                    'username': member['username'],
                    'user_id': member['user_id'],
                    'display_name': member.get('display_name', ''),
                    'type': 'member'
                })

        # Add moderators
        for moderator in members_data.get('moderators', []):
            if moderator.get('username') and moderator.get('user_id'):
                members.append({
                    'username': moderator['username'],
                    'user_id': moderator['user_id'],
                    'display_name': moderator.get('display_name', ''),
                    'type': 'moderator'
                })

        print(f"Loaded {len(members)} community members from {filename}")
        return members

    except Exception as e:
        print(f"Error loading members file {filename}: {e}")
        return None

def get_following_ids(username, max_following=1000):
    """Get list of user IDs that a user is following"""
    print(f"  Fetching following IDs for @{username}")

    following_ids_set = set()  # Use set to prevent duplicates
    cursor = None
    page = 0
    max_pages = 10  # Limit to prevent excessive API calls

    while page < max_pages:
        page += 1

        # URL encode the username to handle special characters
        encoded_username = urllib.parse.quote(username)
        params = f"username={encoded_username}&count=1000"
        if cursor:
            params += f"&cursor={cursor}"

        print(f"    Page {page}: Making request with cursor={cursor}")

        response = make_request("/following-ids", params, username=username)

        if not response:
            print(f"    Page {page}: No response received")
            break

        try:
            # Extract following IDs from response - simple ids array
            if 'ids' in response:
                batch_ids = response['ids']
                new_ids = set(str(id_val) for id_val in batch_ids)
                before_count = len(following_ids_set)
                following_ids_set.update(new_ids)
                after_count = len(following_ids_set)
                print(f"    Page {page}: Got {len(batch_ids)} IDs, {after_count - before_count} new unique IDs")
            else:
                print(f"    Page {page}: No 'ids' field in response")

            # Look for next cursor - only continue if we have a valid cursor (not 0, -1, or None)
            next_cursor_value = response.get('next_cursor')
            print(f"    Page {page}: Next cursor value: {next_cursor_value}")

            # Explicitly check: cursor must exist, not be None, and not be 0 or -1
            if (next_cursor_value is not None and
                isinstance(next_cursor_value, (int, str)) and
                next_cursor_value not in [0, -1, "0", "-1"]):
                cursor = str(next_cursor_value)
                print(f"    Page {page}: Continuing with cursor {cursor}")
            else:
                print(f"    Page {page}: End of pagination (cursor: {next_cursor_value})")
                break

            if len(following_ids_set) >= max_following:  # Configurable limit
                print(f"    Page {page}: Reached ID limit of {max_following}")
                break

        except Exception as e:
            print(f"    Page {page}: Error parsing following IDs for @{username}: {e}")
            break

    following_ids_list = list(following_ids_set)
    print(f"    Found {len(following_ids_list)} unique following IDs for @{username}")
    return following_ids_list

def get_users_by_ids(user_ids):
    """Get user information by user IDs (batch request)"""
    if not user_ids:
        return []

    # Convert list to comma-separated string and URL encode
    user_ids_str = ','.join(str(uid) for uid in user_ids)
    encoded_user_ids = urllib.parse.quote(user_ids_str)

    print(f"  Fetching user info for {len(user_ids)} users")

    params = f"users={encoded_user_ids}"
    response = make_request("/get-users-v2", params, 3)

    if not response:
        return []

    users = []
    try:
        if 'result' in response:
            for user_data in response['result']:
                users.append({
                    'username': user_data.get('screen_name', ''),
                    'user_id': user_data.get('id_str', ''),
                    'display_name': user_data.get('name', ''),
                    'followers_count': user_data.get('followers_count', 0),
                    'following_count': user_data.get('friends_count', 0),
                    'verified': user_data.get('verified', False)
                })

    except Exception as e:
        print(f"    Error parsing users data: {e}")
        print(f"    Response structure: {list(response.keys()) if response else 'None'}")

    print(f"    Retrieved info for {len(users)} users")

    # Show sample of parsed users for debugging
    if users and len(users) <= 5:
        sample_usernames = ', '.join(f'@{u["username"]}' for u in users[:3])
        print(f"    Sample users: {sample_usernames}")
    elif users:
        sample_usernames = ', '.join(f'@{u["username"]}' for u in users[:3])
        print(f"    Sample users: {sample_usernames}...")

    return users

def save_extended_members_checkpoint(community_id, extended_members, raw_data_dir):
    """Save extended members checkpoint"""
    checkpoint_file = os.path.join(raw_data_dir, f"{community_id}_extended_members_checkpoint.json")
    checkpoint_data = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'extended_members': list(extended_members.values()),
        'total_extended_members': len(extended_members)
    }

    os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)

    print(f"Extended members checkpoint saved: {len(extended_members)} users")
    return checkpoint_file

def load_extended_members_checkpoint(community_id, raw_data_dir):
    """Load extended members checkpoint if exists"""
    checkpoint_file = os.path.join(raw_data_dir, f"{community_id}_extended_members_checkpoint.json")

    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)

            extended_members_list = checkpoint_data.get('extended_members', [])
            print(f"Found extended members checkpoint: {len(extended_members_list)} users")

            # Apply current extended_members_limit from config
            config = load_config()
            extended_members_limit = config.get('data', {}).get('extended_members_limit', 100000)

            if len(extended_members_list) > extended_members_limit:
                # Sort by following_count and take top N
                sorted_extended_members = sorted(
                    extended_members_list,
                    key=lambda x: x.get('following_count', 0),
                    reverse=True
                )[:extended_members_limit]
                print(f"Applied current limit: reduced to top {len(sorted_extended_members)} extended members by following_count")
                return sorted_extended_members

            return extended_members_list
        except Exception as e:
            print(f"Error loading extended members checkpoint: {e}")
            return None

    return None

def cleanup_extended_members_checkpoint(community_id, raw_data_dir):
    """Remove extended members checkpoint after successful completion"""
    checkpoint_file = os.path.join(raw_data_dir, f"{community_id}_extended_members_checkpoint.json")

    if os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
            print(f"Extended members checkpoint cleaned up")
        except Exception as e:
            print(f"Warning: Could not remove checkpoint file: {e}")

def build_extended_members(members, community_id, raw_data_dir):
    """Build extended members list by getting all users that members follow"""
    print(f"\nBuilding extended members list from {len(members)} members...")

    # Check for existing checkpoint
    checkpoint_extended_members = load_extended_members_checkpoint(community_id, raw_data_dir)
    if checkpoint_extended_members:
        print(f"Resuming from checkpoint with {len(checkpoint_extended_members)} extended members")
        return checkpoint_extended_members

    # Load config for following limits
    config = load_config()
    max_following_per_user = config.get('data', {}).get('max_following_per_user', 1000)

    extended_members = {}  # Use dict to avoid duplicates

    for i, member in enumerate(members):
        print(f"\nProcessing member {i+1}/{len(members)}: @{member['username']}")

        try:
            # Get IDs of users this member follows
            following_ids = get_following_ids(member['username'], max_following_per_user)

            if following_ids:
                # Process in batches of 100 (API limit)
                batch_size = 100
                for j in range(0, len(following_ids), batch_size):
                    batch_ids = following_ids[j:j+batch_size]

                    # Get user info for this batch
                    batch_users = get_users_by_ids(batch_ids)

                    # Add to extended members (use user_id as key to avoid duplicates)
                    for user in batch_users:
                        if user.get('user_id') and user.get('username'):
                            extended_members[user['user_id']] = {
                                'username': user['username'],
                                'user_id': user['user_id'],
                                'display_name': user.get('display_name', ''),
                                'type': 'extended_member',
                                'followers_count': user.get('followers_count', 0),
                                'following_count': user.get('following_count', 0),
                                'verified': user.get('verified', False)
                            }

                    # Small delay between batches
                    time.sleep(0.5)

        except Exception as e:
            print(f"Error processing member @{member['username']}: {e}")
            continue

    extended_members_list = list(extended_members.values())
    print(f"\nExtended members list built: {len(extended_members_list)} unique users")

    # Rank by following_count and take top users based on config
    config = load_config()
    extended_members_limit = config.get('data', {}).get('extended_members_limit', 100000)

    # Sort by following_count (descending) and take top N
    sorted_extended_members = sorted(
        extended_members_list,
        key=lambda x: x.get('following_count', 0),
        reverse=True
    )[:extended_members_limit]

    print(f"Ranked and limited to top {len(sorted_extended_members)} extended members by following_count")
    if sorted_extended_members:
        highest_following = sorted_extended_members[0].get('following_count', 0)
        lowest_following = sorted_extended_members[-1].get('following_count', 0)
        print(f"Following count range: {highest_following} (highest) to {lowest_following} (lowest)")

    # Save checkpoint with limited list
    limited_extended_members = {user['user_id']: user for user in sorted_extended_members if user.get('user_id')}
    save_extended_members_checkpoint(community_id, limited_extended_members, raw_data_dir)

    return sorted_extended_members

def create_master_list(members, extended_members):
    """Create master list combining members and extended members"""
    print(f"\nCreating master list...")

    # Apply extended_members_limit if needed
    config = load_config()
    extended_members_limit = config.get('data', {}).get('extended_members_limit', 100000)

    if len(extended_members) > extended_members_limit:
        # Sort by following_count and take top N
        limited_extended_members = sorted(
            extended_members,
            key=lambda x: x.get('following_count', 0),
            reverse=True
        )[:extended_members_limit]
        print(f"Applied extended members limit: using top {len(limited_extended_members)} of {len(extended_members)} extended members")
        extended_members = limited_extended_members

    master_dict = {}  # Use dict to avoid duplicates

    # Add all members
    for member in members:
        if member.get('user_id'):
            master_dict[member['user_id']] = member

    # Add extended members (only if not already in members)
    for ext_member in extended_members:
        if ext_member.get('user_id') and ext_member['user_id'] not in master_dict:
            master_dict[ext_member['user_id']] = ext_member

    master_list = list(master_dict.values())
    print(f"Master list created: {len(master_list)} total users")
    print(f"- Original members: {len(members)}")
    print(f"- Extended members: {len(extended_members)}")
    print(f"- Unique total: {len(master_list)}")

    return master_list

def analyze_following_network(master_list):
    """Analyze following relationships within the master list"""
    print(f"\nAnalyzing following network for {len(master_list)} users...")

    # Create lookup dictionaries for efficient searching
    user_id_to_username = {user['user_id']: user['username'] for user in master_list if user.get('user_id')}
    username_to_user_id = {user['username']: user['user_id'] for user in master_list if user.get('username')}
    master_user_ids = set(user_id_to_username.keys())

    following_network = []

    for i, user in enumerate(master_list):
        print(f"\nAnalyzing user {i+1}/{len(master_list)}: @{user['username']}")

        try:
            # Get users this person follows
            config = load_config()
            max_following_per_user = config.get('data', {}).get('max_following_per_user', 1000)
            following_ids = get_following_ids(user['username'], max_following_per_user)

            # Filter to only include users in master list
            following_in_master = []
            for following_id in following_ids:
                if following_id in master_user_ids:
                    following_username = user_id_to_username.get(following_id)
                    if following_username:
                        following_in_master.append(following_username)

            # Add to network
            user_network = {
                'username': user['username'],
                'user_id': user['user_id'],
                'display_name': user.get('display_name', ''),
                'type': user.get('type', 'unknown'),
                'following': following_in_master,
                'following_count_in_network': len(following_in_master)
            }

            following_network.append(user_network)
            print(f"  Found {len(following_in_master)} connections within network")

        except Exception as e:
            print(f"Error analyzing user @{user['username']}: {e}")
            # Add user with empty following list
            following_network.append({
                'username': user['username'],
                'user_id': user['user_id'],
                'display_name': user.get('display_name', ''),
                'type': user.get('type', 'unknown'),
                'following': [],
                'following_count_in_network': 0
            })
            continue

    return following_network

def save_following_network(community_id, following_network, raw_data_dir):
    """Save following network to file"""

    # Calculate summary statistics
    total_connections = sum(user['following_count_in_network'] for user in following_network)
    users_with_connections = sum(1 for user in following_network if user['following_count_in_network'] > 0)

    # Create output data structure
    output_data = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_users_in_network': len(following_network),
            'total_connections': total_connections,
            'users_with_connections': users_with_connections,
            'average_connections_per_user': total_connections / len(following_network) if following_network else 0
        },
        'following_network': following_network
    }

    # Save to file
    filename = os.path.join(raw_data_dir, f"{community_id}_following_network.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\nFollowing network saved to: {filename}")
    print(f"Summary:")
    print(f"- Total users in network: {len(following_network)}")
    print(f"- Total connections: {total_connections}")
    print(f"- Users with connections: {users_with_connections}")
    print(f"- Average connections per user: {total_connections / len(following_network):.2f}")

    return filename, output_data['summary']

def main():
    """Main function - build and analyze following network"""
    global rate_limiter, request_count, start_time

    try:
        # Load configuration
        config = load_config()
        if not config:
            return

        # Initialize rate limiter with config values
        requests_per_second = config['rate_limiting']['requests_per_second']
        rate_limiter = RateLimiter(requests_per_second)

        # Reset counters
        request_count = 0
        start_time = None

        print(f"Following Network Fetcher")
        print(f"Rate limiting: {requests_per_second} requests/second")
        print(f"=" * 60)

        community_ids = config['communities']['ids']
        raw_data_dir = config['output']['raw_data_dir']
        community_delay = config['rate_limiting']['community_delay']

        for i, community_id in enumerate(community_ids):
            print(f"\n{'='*60}")
            print(f"Processing community {i+1}/{len(community_ids)}: {community_id}")
            print(f"{'='*60}")

            # Load members list
            members = load_members_list(community_id, raw_data_dir)
            if not members:
                print(f"Skipping community {community_id} - no members found")
                continue

            # Build extended members list
            extended_members = build_extended_members(members, community_id, raw_data_dir)

            # Create master list
            master_list = create_master_list(members, extended_members)

            # Analyze following network
            following_network = analyze_following_network(master_list)

            # Save results
            save_following_network(community_id, following_network, raw_data_dir)

            # Clean up checkpoint after successful completion
            cleanup_extended_members_checkpoint(community_id, raw_data_dir)

            # Delay between communities
            if i < len(community_ids) - 1:
                print(f"\nWaiting {community_delay} seconds before next community...")
                time.sleep(community_delay)

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'='*60}")
            print(f"FOLLOWING NETWORK ANALYSIS COMPLETE")
            print(f"{'='*60}")
            print(f"Communities processed: {len(community_ids)}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

        print(f"\nOutput files saved in {raw_data_dir}:")
        print(f"- [community_id]_following_network.json")

    except KeyboardInterrupt:
        print(f"\nScript interrupted by user")
        print(f"Extended members checkpoints are preserved in {raw_data_dir} for resuming:")
        print(f"- [community_id]_extended_members_checkpoint.json")

    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"\nExtended members checkpoints are preserved in {raw_data_dir} for resuming:")
        print(f"- [community_id]_extended_members_checkpoint.json")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

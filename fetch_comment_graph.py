#!/usr/bin/env python3
"""
Comment Graph Fetcher

This script analyzes comment interactions within a Twitter/X community by:
1. Loading raw/[community_id]_members.json, raw/[community_id]_non_members.json,
   and raw/[community_id]_following_network.json to create a master list
2. Fetching all community posts using /community-tweets (sorted by Recency)
3. Getting comments for each post using /comments-v2 (sorted by Relevance)
4. Building a comment graph of interactions between users in the master list
5. Saving to raw/[community_id]_comment_graph.json

Uses endpoints:
- /community-tweets?communityId={id}&searchType=Default&rankingMode=Recency&count=1000
- /comments-v2?pid={post_id}&rankingMode=Relevance&count=1000

Rate limited to 10 requests per second to comply with API limits.
"""

import http.client
import json
import os
import toml
from datetime import datetime, timedelta
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
            'data': {'days_back': 730, 'post_limit': 10000},
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
            print(f"API Requests: {request_count}, Rate: {rate:.2f}/sec, Elapsed: {elapsed:.1f}s")

        try:
            conn = http.client.HTTPSConnection("twitter241.p.rapidapi.com")

            headers = {
                'x-rapidapi-key': get_api_key(),
                'x-rapidapi-host': "twitter241.p.rapidapi.com"
            }

            full_endpoint = f"{endpoint}?{params}" if params else endpoint
            # Debug logging for API calls
            if "/comments-v2" in endpoint:
                print(f"        API Call: {endpoint} with params: {params[:100]}...")

            conn.request("GET", full_endpoint, headers=headers)

            res = conn.getresponse()
            data = res.read()
            conn.close()

            if res.status == 200:
                response_data = json.loads(data.decode("utf-8"))
                # Debug logging for response structure
                if "/comments-v2" in endpoint and response_data:
                    if 'result' in response_data:
                        result = response_data['result']
                        instructions = result.get('instructions', []) if 'instructions' in result else []
                        print(f"        API Response: {len(instructions)} instructions")
                        for i, instruction in enumerate(instructions[:3]):  # Log first 3 instructions
                            inst_type = instruction.get('type', 'unknown')
                            entries_count = len(instruction.get('entries', [])) if 'entries' in instruction else 0
                            print(f"          Instruction {i}: {inst_type} with {entries_count} entries")
                    else:
                        print(f"        API Response: No result field - keys: {list(response_data.keys())}")
                return response_data
            elif res.status == 429:  # Rate limit exceeded
                if attempt < max_retries - 1:
                    backoff_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    print(f"        Rate limit hit, waiting {backoff_time}s before retry (attempt {attempt + 1}/{max_retries})")
                    time.sleep(backoff_time)
                    continue
                else:
                    print(f"        Rate limit exceeded, max retries reached")
                    return None
            elif res.status >= 500:  # Server errors
                if attempt < max_retries - 1:
                    backoff_time = 2 ** attempt
                    print(f"        Server error {res.status}, retrying in {backoff_time}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(backoff_time)
                    continue
                else:
                    print(f"        Error: HTTP {res.status} - {data.decode('utf-8')[:200]}...")
                    return None
            else:
                error_msg = data.decode('utf-8')[:200] if data else 'No response body'
                print(f"        Error: HTTP {res.status} - {error_msg}...")
                return None

        except json.JSONDecodeError as e:
            print(f"        JSON decode error: {str(e)}")
            if data:
                print(f"        Raw response (first 200 chars): {data.decode('utf-8', errors='ignore')[:200]}...")
            if attempt < max_retries - 1:
                backoff_time = 2 ** attempt
                print(f"        Retrying in {backoff_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(backoff_time)
                continue
            else:
                return None
        except Exception as e:
            if attempt < max_retries - 1:
                backoff_time = 2 ** attempt
                print(f"        Request failed: {str(e)}, retrying in {backoff_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(backoff_time)
                continue
            else:
                print(f"        Request failed after {max_retries} attempts: {str(e)}")
                return None

    return None

def load_master_list(community_id, raw_data_dir):
    """Load and combine all user lists to create master list"""
    master_users = {}  # Use dict to avoid duplicates

    # Load members
    members_file = os.path.join(raw_data_dir, f"{community_id}_members.json")
    if os.path.exists(members_file):
        try:
            with open(members_file, 'r', encoding='utf-8') as f:
                members_data = json.load(f)

            # Add regular members
            members_count = 0
            for member in members_data.get('members', []):
                if member.get('username') and member.get('user_id'):
                    master_users[member['user_id']] = {
                        'username': member['username'],
                        'user_id': member['user_id'],
                        'display_name': member.get('display_name', ''),
                        'type': 'member'
                    }
                    members_count += 1

            # Add moderators
            moderators_count = 0
            for moderator in members_data.get('moderators', []):
                if moderator.get('username') and moderator.get('user_id'):
                    master_users[moderator['user_id']] = {
                        'username': moderator['username'],
                        'user_id': moderator['user_id'],
                        'display_name': moderator.get('display_name', ''),
                        'type': 'moderator'
                    }
                    moderators_count += 1

            print(f"Loaded {members_count} members and {moderators_count} moderators")

        except FileNotFoundError:
            print(f"Members file not found: {members_file}")
        except json.JSONDecodeError as e:
            print(f"Error parsing members JSON file: {e}")
        except Exception as e:
            print(f"Error loading members file: {e}")
    else:
        print(f"Members file not found: {members_file}")

    # Load non-members
    non_members_file = os.path.join(raw_data_dir, f"{community_id}_non_members.json")
    if os.path.exists(non_members_file):
        try:
            with open(non_members_file, 'r', encoding='utf-8') as f:
                non_members_data = json.load(f)

            for non_member in non_members_data.get('non_members', []):
                if non_member.get('user_id') and non_member['user_id'] not in master_users:
                    master_users[non_member['user_id']] = {
                        'username': non_member.get('username', ''),
                        'user_id': non_member['user_id'],
                        'display_name': non_member.get('display_name', ''),
                        'type': 'non_member'
                    }

            print(f"Added {len([nm for nm in non_members_data.get('non_members', []) if nm.get('user_id') and nm['user_id'] not in master_users])} non-members")

        except Exception as e:
            print(f"Error loading non-members file: {e}")
    else:
        print(f"Non-members file not found: {non_members_file}")

    # Load following network
    following_network_file = os.path.join(raw_data_dir, f"{community_id}_following_network.json")
    if os.path.exists(following_network_file):
        try:
            with open(following_network_file, 'r', encoding='utf-8') as f:
                following_data = json.load(f)

            added_count = 0
            for user in following_data.get('following_network', []):
                if user.get('user_id') and user['user_id'] not in master_users:
                    master_users[user['user_id']] = {
                        'username': user.get('username', ''),
                        'user_id': user['user_id'],
                        'display_name': user.get('display_name', ''),
                        'type': user.get('type', 'extended_member')
                    }
                    added_count += 1

            print(f"Added {added_count} users from following network")

        except Exception as e:
            print(f"Error loading following network file: {e}")
    else:
        print(f"Following network file not found: {following_network_file}")

    master_list = list(master_users.values())
    print(f"Master list created with {len(master_list)} unique users")

    # Create lookup dictionaries for fast searching
    user_id_lookup = {user['user_id']: user for user in master_list}
    username_lookup = {user['username'].lower(): user for user in master_list if user.get('username')}

    return master_list, user_id_lookup, username_lookup

def is_post_within_days(post_data, days_back):
    """Check if post is within the specified days back"""
    try:
        if 'created_at' in post_data:
            created_at = post_data['created_at']
        elif 'tweet' in post_data and 'legacy' in post_data['tweet']:
            created_at = post_data['tweet']['legacy'].get('created_at')
        elif 'legacy' in post_data:
            created_at = post_data['legacy'].get('created_at')
        else:
            return False

        if not created_at:
            return False

        # Parse Twitter's date format
        post_date = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
        cutoff_date = datetime.now(post_date.tzinfo) - timedelta(days=days_back)

        return post_date >= cutoff_date
    except Exception as e:
        return False

def get_community_posts(community_id, days_back):
    """Get all posts from a community within the specified time range"""
    print(f"Fetching community posts for {community_id}")

    posts = []
    cursor = None
    page = 0
    max_pages = 50  # Reasonable limit

    while page < max_pages:
        page += 1

        params = f"communityId={community_id}&searchType=Default&rankingMode=Recency&count=1000"
        if cursor:
            params += f"&cursor={cursor}"

        response = make_request("/community-tweets", params)

        if not response:
            break

        found_posts = False

        try:
            if 'result' in response and 'timeline' in response['result']:
                timeline = response['result']['timeline']
                if 'instructions' in timeline:
                    for instruction in timeline['instructions']:
                        if instruction.get('type') == 'TimelineAddEntries' and 'entries' in instruction:
                            for entry in instruction['entries']:
                                entry_id = entry.get('entryId', '')
                                if 'tweet-' in entry_id:
                                    entry_content = entry.get('content', {})
                                    if 'itemContent' in entry_content and 'tweet_results' in entry_content['itemContent']:
                                        tweet_data = entry_content['itemContent']['tweet_results'].get('result', {})

                                        # Check if within date range
                                        if is_post_within_days(tweet_data, days_back):
                                            post_info = extract_post_info(tweet_data)
                                            if post_info:
                                                posts.append(post_info)
                                                found_posts = True
                                        else:
                                            # If we hit content outside date range, stop
                                            print(f"  Reached posts outside {days_back} days range")
                                            return posts
                                elif 'tile-' in entry_id:
                                    # Handle tile entries (like spaces/events) - skip them
                                    continue

            # Look for cursor for next page
            cursor = None
            if 'result' in response and 'timeline' in response['result']:
                timeline = response['result']['timeline']
                if 'instructions' in timeline:
                    for instruction in timeline['instructions']:
                        if instruction.get('type') == 'TimelineAddEntries':
                            entries = instruction.get('entries', [])
                            for entry in entries:
                                if entry.get('entryId', '').startswith('cursor-bottom-'):
                                    cursor_content = entry.get('content', {})
                                    if 'value' in cursor_content:
                                        cursor = cursor_content['value']
                                        break

            # Also check for cursor at the top level
            if not cursor and 'cursor' in response and 'bottom' in response['cursor']:
                cursor = response['cursor']['bottom']

            if not cursor or not found_posts:
                break

            print(f"  Page {page}: Found {len(posts)} total posts so far")

        except Exception as e:
            print(f"  Error parsing community posts page {page}: {e}")
            break

    print(f"Found {len(posts)} community posts within {days_back} days")
    return posts

def extract_post_info(post_data):
    """Extract relevant information from a post"""
    try:
        # Handle TweetWithVisibilityResults wrapper for community tweets
        if post_data.get('__typename') == 'TweetWithVisibilityResults' and 'tweet' in post_data:
            tweet_data = post_data['tweet']
        else:
            tweet_data = post_data

        # Extract legacy data and core data
        if 'legacy' in tweet_data:
            legacy = tweet_data['legacy']
            core = tweet_data.get('core', {})
        else:
            return None

        # Extract user data
        user_data = {}
        if 'user_results' in core and 'result' in core['user_results']:
            user_result = core['user_results']['result']
            if 'legacy' in user_result:
                user_data = user_result['legacy']

        return {
            'post_id': legacy.get('id_str', ''),
            'text': legacy.get('full_text', ''),
            'created_at': legacy.get('created_at', ''),
            'user_id': legacy.get('user_id_str', ''),
            'username': user_data.get('screen_name', ''),
            'display_name': user_data.get('name', ''),
            'retweet_count': legacy.get('retweet_count', 0),
            'favorite_count': legacy.get('favorite_count', 0),
            'reply_count': legacy.get('reply_count', 0)
        }

    except Exception as e:
        print(f"Error extracting post info: {e}")
        return None

def analyze_response_structure(response, indent=""):
    """Helper function to analyze and log API response structure for debugging"""
    try:
        if not response or not isinstance(response, dict):
            print(f"{indent}Invalid response: {type(response)}")
            return

        print(f"{indent}Response keys: {list(response.keys())}")

        if 'result' in response:
            result = response['result']
            print(f"{indent}Result keys: {list(result.keys())}")

            if 'instructions' in result:
                instructions = result['instructions']
                print(f"{indent}Instructions count: {len(instructions)}")

                for i, instruction in enumerate(instructions[:2]):  # Analyze first 2 instructions
                    print(f"{indent}Instruction {i}:")
                    print(f"{indent}  Type: {instruction.get('type')}")

                    if 'entries' in instruction:
                        entries = instruction['entries']
                        print(f"{indent}  Entries count: {len(entries)}")

                        for j, entry in enumerate(entries[:3]):  # Analyze first 3 entries
                            entry_id = entry.get('entryId', 'No ID')
                            print(f"{indent}    Entry {j}: {entry_id}")

                            if 'content' in entry:
                                content = entry['content']
                                print(f"{indent}      Content keys: {list(content.keys())}")

                                if 'itemContent' in content:
                                    item_content = content['itemContent']
                                    print(f"{indent}        ItemContent keys: {list(item_content.keys())}")

                                    if 'tweet_results' in item_content:
                                        tweet_results = item_content['tweet_results']
                                        print(f"{indent}          TweetResults keys: {list(tweet_results.keys())}")

                                        if 'result' in tweet_results:
                                            tweet_data = tweet_results['result']
                                            print(f"{indent}            Tweet data keys: {list(tweet_data.keys())}")

                                            # Check for TweetWithVisibilityResults wrapper
                                            if '__typename' in tweet_data:
                                                print(f"{indent}            __typename: {tweet_data['__typename']}")

                                            if 'tweet' in tweet_data:
                                                inner_tweet = tweet_data['tweet']
                                                print(f"{indent}            Inner tweet keys: {list(inner_tweet.keys())}")

                                if 'items' in content:  # For conversation threads
                                    items = content['items']
                                    print(f"{indent}        Thread items count: {len(items)}")

                            if j >= 2:  # Limit output
                                break

                    if i >= 1:  # Limit output
                        break

        if 'cursor' in response:
            print(f"{indent}Top-level cursor keys: {list(response['cursor'].keys())}")

    except Exception as e:
        print(f"{indent}Error analyzing response structure: {e}")

def get_post_comments(post_id):
    """Get comments for a specific post with improved error handling and debug logging"""
    comments = []
    cursor = None
    page = 0
    max_pages = 10  # Reasonable limit per post

    print(f"    Fetching comments for post {post_id}...")

    while page < max_pages:
        page += 1
        print(f"      Processing comments page {page}...")

        params = f"pid={post_id}&rankingMode=Relevance&count=1000"
        if cursor:
            params += f"&cursor={cursor}"

        response = make_request("/comments-v2", params)

        if not response:
            print(f"      No response received for page {page}")
            break

        # Debug: Analyze response structure on first page
        if page == 1:
            print(f"      Analyzing response structure for debugging:")
            analyze_response_structure(response, "        ")

        found_comments = False
        processed_entries = 0

        try:
            if 'result' in response and 'instructions' in response['result']:
                instructions = response['result']['instructions']
                print(f"      Found {len(instructions)} instructions")

                for instruction_idx, instruction in enumerate(instructions):
                    if instruction.get('type') == 'TimelineAddEntries' and 'entries' in instruction:
                        entries = instruction['entries']
                        print(f"        Instruction {instruction_idx}: TimelineAddEntries with {len(entries)} entries")

                        for entry_idx, entry in enumerate(entries):
                            entry_id = entry.get('entryId', '')
                            processed_entries += 1

                            if 'tweet-' in entry_id:
                                print(f"          Processing tweet entry {entry_idx}: {entry_id}")
                                entry_content = entry.get('content', {})

                                if 'itemContent' in entry_content and 'tweet_results' in entry_content['itemContent']:
                                    tweet_results = entry_content['itemContent']['tweet_results']
                                    comment_data = tweet_results.get('result', {})

                                    if comment_data:
                                        print(f"            Found comment data with keys: {list(comment_data.keys())}")
                                        comment_info = extract_comment_info(comment_data, post_id)
                                        if comment_info:
                                            comments.append(comment_info)
                                            found_comments = True
                                        else:
                                            print(f"            Failed to extract comment info")
                                    else:
                                        print(f"            No result data in tweet_results")
                                else:
                                    print(f"            No tweet_results in itemContent")

                            elif 'conversationthread-' in entry_id:
                                print(f"          Processing conversation thread {entry_idx}: {entry_id}")
                                # Handle conversation threads that contain multiple comments
                                if 'content' in entry and 'items' in entry['content']:
                                    thread_items = entry['content']['items']
                                    print(f"            Thread has {len(thread_items)} items")

                                    for item_idx, item in enumerate(thread_items):
                                        if 'item' in item and 'itemContent' in item['item']:
                                            item_content = item['item']['itemContent']
                                            if 'tweet_results' in item_content:
                                                tweet_results = item_content['tweet_results']
                                                comment_data = tweet_results.get('result', {})

                                                if comment_data:
                                                    print(f"              Processing thread item {item_idx}")
                                                    comment_info = extract_comment_info(comment_data, post_id)
                                                    if comment_info:
                                                        comments.append(comment_info)
                                                        found_comments = True
                            else:
                                # Log other entry types for debugging
                                if not entry_id.startswith('cursor-'):
                                    print(f"          Skipping entry {entry_idx}: {entry_id}")

            # Look for cursor for next page
            cursor = None
            if 'result' in response and 'instructions' in response['result']:
                instructions = response['result']['instructions']
                for instruction in instructions:
                    if instruction.get('type') == 'TimelineAddEntries':
                        entries = instruction.get('entries', [])
                        for entry in entries:
                            if entry.get('entryId', '').startswith('cursor-bottom-'):
                                cursor_content = entry.get('content', {})
                                if 'value' in cursor_content:
                                    cursor = cursor_content['value']
                                    print(f"      Found cursor for next page: {cursor[:50] if cursor else 'None'}...")
                                    break

            # Also check for cursor at the top level
            if not cursor and 'cursor' in response and 'bottom' in response['cursor']:
                cursor = response['cursor']['bottom']
                print(f"      Found top-level cursor: {cursor[:50] if cursor else 'None'}...")

            print(f"      Page {page} results: processed {processed_entries} entries, found {len([c for c in comments if c])} new comments")

            if not cursor or not found_comments:
                print(f"      Stopping: cursor={'Yes' if cursor else 'No'}, found_comments={found_comments}")
                break

        except Exception as e:
            print(f"      Error parsing comments page {page} for post {post_id}: {e}")
            import traceback
            traceback.print_exc()
            break

    if comments:
        print(f"    Collected {len(comments)} total comments from {page} pages for post {post_id}")
    else:
        print(f"    No comments found for post {post_id} after {page} pages")

    return comments

def extract_comment_info(comment_data, original_post_id):
    """Extract relevant information from a comment - handles TweetWithVisibilityResults wrapper"""
    try:
        # Handle TweetWithVisibilityResults wrapper and multiple nesting patterns
        tweet_data = comment_data
        wrapper_found = False

        # Check for TweetWithVisibilityResults wrapper by typename
        if '__typename' in comment_data and comment_data['__typename'] == 'TweetWithVisibilityResults':
            print(f"    Debug: Found TweetWithVisibilityResults by __typename")
            if 'tweet' in comment_data:
                tweet_data = comment_data['tweet']
                wrapper_found = True

        # Check for tweet field containing the actual tweet data
        elif 'tweet' in comment_data and isinstance(comment_data['tweet'], dict):
            if 'legacy' in comment_data['tweet']:
                print(f"    Debug: Found tweet wrapper with legacy data")
                tweet_data = comment_data['tweet']
                wrapper_found = True

        # Check for result wrapper (sometimes tweets are wrapped in result)
        elif 'result' in comment_data and isinstance(comment_data['result'], dict):
            result_data = comment_data['result']
            if '__typename' in result_data and result_data['__typename'] == 'Tweet':
                print(f"    Debug: Found result wrapper with Tweet typename")
                tweet_data = result_data
            elif 'legacy' in result_data:
                print(f"    Debug: Found result wrapper with legacy data")
                tweet_data = result_data

        if not wrapper_found:
            print(f"    Debug: Processing direct tweet data")

        # Extract legacy data (main tweet content) with multiple fallbacks
        legacy = None
        if 'legacy' in tweet_data:
            legacy = tweet_data['legacy']
        elif 'tweet' in tweet_data and 'legacy' in tweet_data['tweet']:
            # Sometimes there's another level of nesting
            legacy = tweet_data['tweet']['legacy']
            print(f"    Debug: Found legacy data in nested tweet structure")
        else:
            print(f"    Debug: No legacy data found in tweet_data keys: {list(tweet_data.keys())}")
            return None

        # Extract core data (user information)
        core = tweet_data.get('core', {})

        # Extract user data with comprehensive fallback methods
        user_data = {}
        user_source = ""

        # Method 1: From core.user_results.result.legacy
        if not user_data and 'user_results' in core and 'result' in core['user_results']:
            user_result = core['user_results']['result']
            if 'legacy' in user_result:
                user_data = user_result['legacy']
                user_source = "core.user_results.result.legacy"
            elif 'screen_name' in user_result or 'name' in user_result:
                user_data = user_result
                user_source = "core.user_results.result (direct)"

        # Method 2: From core.user_result.result (alternative field name)
        if not user_data and 'user_result' in core and 'result' in core['user_result']:
            user_result = core['user_result']['result']
            if 'legacy' in user_result:
                user_data = user_result['legacy']
                user_source = "core.user_result.result.legacy"

        # Method 3: From top-level user field
        if not user_data and 'user' in tweet_data:
            user_obj = tweet_data['user']
            if isinstance(user_obj, dict):
                if 'legacy' in user_obj:
                    user_data = user_obj['legacy']
                    user_source = "tweet_data.user.legacy"
                elif 'result' in user_obj and 'legacy' in user_obj['result']:
                    user_data = user_obj['result']['legacy']
                    user_source = "tweet_data.user.result.legacy"
                elif 'screen_name' in user_obj or 'name' in user_obj:
                    user_data = user_obj
                    user_source = "tweet_data.user (direct)"

        # Method 4: From legacy.user (fallback for older API responses)
        if not user_data and 'user' in legacy:
            user_data = legacy['user']
            user_source = "legacy.user"

        # Method 5: Try to extract from user_id_str in legacy and cross-reference
        if not user_data and 'user_id_str' in legacy:
            # Create minimal user data from available info
            user_data = {
                'id_str': legacy['user_id_str'],
                'screen_name': '',  # Will be empty but at least we have the ID
                'name': ''
            }
            user_source = "legacy.user_id_str (minimal)"

        # Extract user ID with fallbacks
        user_id = None
        if user_data:
            user_id = user_data.get('id_str') or user_data.get('id') or legacy.get('user_id_str')
        else:
            user_id = legacy.get('user_id_str')

        # Create comment info with robust field extraction
        comment_info = {
            'comment_id': legacy.get('id_str', ''),
            'text': legacy.get('full_text', '') or legacy.get('text', ''),
            'created_at': legacy.get('created_at', ''),
            'commenter_user_id': user_id or '',
            'commenter_username': user_data.get('screen_name', '') if user_data else '',
            'commenter_display_name': user_data.get('name', '') if user_data else '',
            'original_post_id': original_post_id,
            'in_reply_to_status_id': legacy.get('in_reply_to_status_id_str'),
            'in_reply_to_user_id': legacy.get('in_reply_to_user_id_str'),
            'favorite_count': legacy.get('favorite_count', 0),
            'retweet_count': legacy.get('retweet_count', 0)
        }

        # Debug logging
        if comment_info['commenter_user_id']:
            print(f"    Debug: Successfully extracted comment {comment_info['comment_id']} from user {comment_info['commenter_username']} ({comment_info['commenter_user_id']}) via {user_source}")
        else:
            print(f"    Debug: Warning - no commenter_user_id found in comment {comment_info['comment_id']}")
            print(f"    Debug: Available legacy keys: {list(legacy.keys())}")
            if user_data:
                print(f"    Debug: Available user_data keys: {list(user_data.keys())}")

        # Validate that we have minimum required data
        if not comment_info['comment_id']:
            print(f"    Debug: Warning - no comment_id found, skipping")
            return None

        return comment_info

    except Exception as e:
        print(f"    Error extracting comment info: {e}")
        print(f"    Debug: comment_data type: {type(comment_data)}")
        if isinstance(comment_data, dict):
            print(f"    Debug: comment_data keys: {list(comment_data.keys())}")
            if '__typename' in comment_data:
                print(f"    Debug: __typename: {comment_data['__typename']}")
        import traceback
        traceback.print_exc()
        return None

def build_comment_graph(posts, user_id_lookup, days_back):
    """Build comment graph from posts and their comments with enhanced logging"""
    comment_graph = []
    total_posts = len(posts)
    master_list_size = len(user_id_lookup)

    print(f"Building comment graph from {total_posts} posts...")
    print(f"Master list contains {master_list_size} users for filtering")

    total_comments_found = 0
    total_interactions_added = 0

    for i, post in enumerate(posts):
        print(f"  Processing post {i + 1}/{total_posts}: {post['post_id']} by @{post.get('username', 'unknown')}")

        # Get comments for this post
        comments = get_post_comments(post['post_id'])
        total_comments_found += len(comments)

        if comments:
            print(f"    Found {len(comments)} comments for post {post['post_id']}")

            comments_in_master_list = 0
            for comment in comments:
                # Check if commenter is in master list
                commenter_id = comment.get('commenter_user_id')
                commenter_username = comment.get('commenter_username', '')

                if commenter_id and commenter_id in user_id_lookup:
                    commenter_info = user_id_lookup[commenter_id]
                    comments_in_master_list += 1

                    # Create comment graph entry
                    graph_entry = {
                        'commenter_user_id': commenter_id,
                        'commenter_username': comment.get('commenter_username', ''),
                        'commenter_display_name': comment.get('commenter_display_name', ''),
                        'commenter_type': commenter_info.get('type', 'unknown'),
                        'comment_id': comment.get('comment_id', ''),
                        'comment_text': comment.get('text', ''),
                        'comment_created_at': comment.get('created_at', ''),
                        'original_post_id': post['post_id'],
                        'original_post_author_id': post.get('user_id', ''),
                        'original_post_author_username': post.get('username', ''),
                        'original_post_text': post.get('text', ''),
                        'original_post_created_at': post.get('created_at', ''),
                        'in_reply_to_status_id': comment.get('in_reply_to_status_id'),
                        'in_reply_to_user_id': comment.get('in_reply_to_user_id'),
                        'comment_favorite_count': comment.get('favorite_count', 0),
                        'comment_retweet_count': comment.get('retweet_count', 0)
                    }

                    comment_graph.append(graph_entry)
                    total_interactions_added += 1

                    print(f"      Added interaction: @{commenter_username} ({commenter_info.get('type', 'unknown')}) -> post by @{post.get('username', 'unknown')}")
                else:
                    if commenter_id:
                        print(f"      Skipping comment from @{commenter_username} ({commenter_id}) - not in master list")
                    else:
                        print(f"      Skipping comment - no commenter_user_id found")

            if comments_in_master_list == 0:
                print(f"      No comments from users in master list")
            else:
                print(f"      {comments_in_master_list}/{len(comments)} comments from users in master list")
        else:
            print(f"    No comments found for post {post['post_id']}")

        # Small delay between posts to be nice to the API
        time.sleep(0.1)

    print(f"\n{'='*60}")
    print(f"COMMENT GRAPH BUILDING SUMMARY")
    print(f"{'='*60}")
    print(f"Posts processed: {total_posts}")
    print(f"Total comments found: {total_comments_found}")
    print(f"Comments from master list users: {total_interactions_added}")
    print(f"Master list size: {master_list_size}")
    print(f"Final comment graph size: {len(comment_graph)} interactions")

    return comment_graph

def save_comment_graph(community_id, comment_graph, raw_data_dir):
    """Save comment graph to file"""

    # Calculate summary statistics
    total_comments = len(comment_graph)
    unique_commenters = len(set(entry['commenter_user_id'] for entry in comment_graph))
    unique_posts = len(set(entry['original_post_id'] for entry in comment_graph))

    # Group by commenter type
    type_breakdown = {}
    for entry in comment_graph:
        commenter_type = entry['commenter_type']
        type_breakdown[commenter_type] = type_breakdown.get(commenter_type, 0) + 1

    # Create output data structure
    output_data = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_comment_interactions': total_comments,
            'unique_commenters': unique_commenters,
            'unique_posts_with_comments': unique_posts,
            'commenter_type_breakdown': type_breakdown
        },
        'comment_graph': comment_graph
    }

    # Save to file
    filename = os.path.join(raw_data_dir, f"{community_id}_comment_graph.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\nComment graph saved to: {filename}")
    print(f"Summary:")
    print(f"- Total comment interactions: {total_comments}")
    print(f"- Unique commenters: {unique_commenters}")
    print(f"- Unique posts with comments: {unique_posts}")
    print(f"- Commenter type breakdown: {type_breakdown}")

    return filename, output_data['summary']

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
                "retweet_count": 2
            },
            "core": {
                "user_results": {
                    "result": {
                        "legacy": {
                            "screen_name": "testuser",
                            "name": "Test User"
                        }
                    }
                }
            }
        }
    }

    # Test direct tweet data
    test_data_direct = {
        "legacy": {
            "id_str": "0987654321",
            "full_text": "Direct tweet comment",
            "created_at": "Mon Jan 02 00:00:00 +0000 2024",
            "user_id_str": "123456789",
            "favorite_count": 3,
            "retweet_count": 1
        },
        "core": {
            "user_results": {
                "result": {
                    "legacy": {
                        "screen_name": "directuser",
                        "name": "Direct User"
                    }
                }
            }
        }
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
    """Main function - build comment graph from community posts"""
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

        print(f"Comment Graph Fetcher")
        print(f"Rate limiting: {requests_per_second} requests/second")
        print(f"=" * 60)

        community_ids = config['communities']['ids']
        raw_data_dir = config['output']['raw_data_dir']
        days_back = config['data']['days_back']
        community_delay = config['rate_limiting']['community_delay']

        for i, community_id in enumerate(community_ids):
            print(f"\n{'='*60}")
            print(f"Processing community {i+1}/{len(community_ids)}: {community_id}")
            print(f"{'='*60}")

            # Load master list from all sources
            try:
                master_list, user_id_lookup, username_lookup = load_master_list(community_id, raw_data_dir)
                if not master_list:
                    print(f"Skipping community {community_id} - no users found")
                    continue
            except Exception as e:
                print(f"Error loading data for community {community_id}: {e}")
                continue

            # Get community posts
            try:
                posts = get_community_posts(community_id, days_back)
                if not posts:
                    print(f"No posts found for community {community_id}")
                    continue
            except Exception as e:
                print(f"Error fetching posts for community {community_id}: {e}")
                continue

            # Build comment graph
            try:
                comment_graph = build_comment_graph(posts, user_id_lookup, days_back)

                # Save comment graph
                save_comment_graph(community_id, comment_graph, raw_data_dir)
            except Exception as e:
                print(f"Error building comment graph for community {community_id}: {e}")
                continue

            # Delay between communities
            if i < len(community_ids) - 1:
                print(f"\nWaiting {community_delay} seconds before next community...")
                time.sleep(community_delay)

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'='*60}")
            print(f"COMMENT GRAPH ANALYSIS COMPLETE")
            print(f"{'='*60}")
            print(f"Communities processed: {len(community_ids)}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

        print(f"\nOutput files saved in {raw_data_dir}:")
        print(f"- [community_id]_comment_graph.json")

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

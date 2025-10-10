#!/usr/bin/env python3
"""
Non-Member Interactions Fetcher

This script fetches interactions from non-members directed toward the community by:
1. Loading raw/[community_id]_members.json and raw/[community_id]_non_members.json
2. Creating a master list from these 2 lists
3. Going through all non-members and finding posts that:
   - Mention a user from master list (@username)
   - Are retweets/quotes of posts made by someone from master list
4. Going through all replies of non-members and keeping only replies that reply to posts made by someone from master list
5. Saving to raw/[community_id]_non_member_interactions.json

Uses endpoints:
- /user-tweets for non-member timeline posts
- /user-replies-v2 for non-member replies

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
import re
import glob

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
        print("Error: config.toml not found")
        return None
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
                    print(f"Error: HTTP {res.status} - {data.decode('utf-8')}")
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

def save_checkpoint(community_id, processed_non_members, raw_data_dir):
    """Save checkpoint data to resume processing later"""
    checkpoint_file = os.path.join(raw_data_dir, f"{community_id}_non_member_interactions_checkpoint.json")
    checkpoint_data = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'processed_non_members': processed_non_members,
        'total_processed': len(processed_non_members)
    }

    os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)

    print(f"Checkpoint saved: {len(processed_non_members)} non-members processed")

def load_checkpoint(community_id, raw_data_dir):
    """Load checkpoint data to resume processing"""
    checkpoint_file = os.path.join(raw_data_dir, f"{community_id}_non_member_interactions_checkpoint.json")

    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)

            print(f"Found checkpoint: {len(checkpoint_data.get('processed_non_members', []))} non-members already processed")
            return checkpoint_data.get('processed_non_members', [])
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
            return []

    return []

def cleanup_checkpoint(community_id, raw_data_dir):
    """Remove checkpoint file after successful completion"""
    checkpoint_file = os.path.join(raw_data_dir, f"{community_id}_non_member_interactions_checkpoint.json")

    if os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
            print(f"Checkpoint file cleaned up")
        except Exception as e:
            print(f"Warning: Could not remove checkpoint file: {e}")

def get_processed_usernames(processed_non_members):
    """Extract usernames from processed non-members list"""
    return {member.get('username', '').lower() for member in processed_non_members if member.get('username')}


def load_members_and_non_members(community_id, raw_data_dir):
    """Load both members and non-members lists to create master list"""

    # Load members
    members_file = os.path.join(raw_data_dir, f"{community_id}_members.json")
    if not os.path.exists(members_file):
        print(f"Error: Members file not found: {members_file}")
        return None

    # Load non-members
    non_members_file = os.path.join(raw_data_dir, f"{community_id}_non_members.json")
    if not os.path.exists(non_members_file):
        print(f"Error: Non-members file not found: {non_members_file}")
        return None

    try:
        # Load members data
        with open(members_file, 'r', encoding='utf-8') as f:
            members_data = json.load(f)

        # Load non-members data
        with open(non_members_file, 'r', encoding='utf-8') as f:
            non_members_data = json.load(f)

        # Create master list
        master_list = {
            'user_ids': set(),
            'usernames': set(),
            'members': [],
            'non_members': []
        }

        # Add members to master list
        for member in members_data.get('members', []):
            if member.get('user_id') and member.get('username'):
                master_list['user_ids'].add(member['user_id'])
                master_list['usernames'].add(member['username'].lower())
                master_list['members'].append({
                    'user_id': member['user_id'],
                    'username': member['username'],
                    'display_name': member.get('display_name', ''),
                    'type': 'member'
                })

        # Add moderators to master list
        for moderator in members_data.get('moderators', []):
            if moderator.get('user_id') and moderator.get('username'):
                master_list['user_ids'].add(moderator['user_id'])
                master_list['usernames'].add(moderator['username'].lower())
                master_list['members'].append({
                    'user_id': moderator['user_id'],
                    'username': moderator['username'],
                    'display_name': moderator.get('display_name', ''),
                    'type': 'moderator'
                })

        # Add non-members to master list (these are users that members interact with)
        for non_member in non_members_data.get('non_members', []):
            if non_member.get('user_id') or non_member.get('username'):
                if non_member.get('user_id'):
                    master_list['user_ids'].add(non_member['user_id'])
                if non_member.get('username'):
                    master_list['usernames'].add(non_member['username'].lower())
                master_list['non_members'].append({
                    'user_id': non_member.get('user_id'),
                    'username': non_member.get('username'),
                    'display_name': non_member.get('display_name', ''),
                    'type': 'non_member',
                    'interaction_counts': non_member.get('interaction_counts', {})
                })

        print(f"Master list created:")
        print(f"- Members: {len(master_list['members'])}")
        print(f"- Non-members: {len(master_list['non_members'])}")
        print(f"- Total unique users: {len(master_list['user_ids'])}")

        return master_list, non_members_data.get('non_members', [])

    except Exception as e:
        print(f"Error loading files: {e}")
        return None

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

def extract_mentions_from_text(text):
    """Extract @mentions from post text"""
    if not text:
        return []

    # Find all @mentions in the text
    mentions = re.findall(r'@(\w+)', text)
    # Convert to lowercase for consistency
    return [mention.lower() for mention in mentions]

def extract_post_data(post_data, master_list):
    """Extract relevant data from a post/tweet and check if it's relevant to master list"""
    try:
        # Handle different data structures
        if 'tweet' in post_data and 'legacy' in post_data['tweet']:
            legacy = post_data['tweet']['legacy']
            core = post_data['tweet'].get('core', {})
            tweet_data = post_data['tweet']
        elif 'legacy' in post_data:
            legacy = post_data['legacy']
            core = post_data.get('core', {})
            tweet_data = post_data
        else:
            return None

        user_data = core.get('user_results', {}).get('result', {}).get('legacy', {})

        # Check for community context
        community_id = None
        is_community_post = False

        # Look for community information
        if 'community' in tweet_data:
            community_info = tweet_data['community']
            if isinstance(community_info, dict) and 'id_str' in community_info:
                community_id = community_info['id_str']
                is_community_post = True

        # Check in tweet_data root level for community markers
        if not community_id:
            for key in ['community_results', 'communities', 'community_note']:
                if key in tweet_data:
                    community_data = tweet_data[key]
                    if isinstance(community_data, dict):
                        if 'result' in community_data and 'rest_id' in community_data['result']:
                            community_id = community_data['result']['rest_id']
                            is_community_post = True
                            break
                        elif 'id_str' in community_data:
                            community_id = community_data['id_str']
                            is_community_post = True
                            break

        # Extract basic post information
        extracted_data = {
            'post_id': legacy.get('id_str', ''),
            'text': legacy.get('full_text', ''),
            'created_at': legacy.get('created_at', ''),
            'user_id': legacy.get('user_id_str', ''),
            'username': user_data.get('screen_name', ''),
            'is_community_post': is_community_post,
            'community_id': community_id,
            'is_retweet': legacy.get('retweeted_status_id_str') is not None,
            'is_reply': legacy.get('in_reply_to_status_id_str') is not None,
            'is_quote': legacy.get('quoted_status_id_str') is not None,
            'reply_to_post_id': legacy.get('in_reply_to_status_id_str'),
            'reply_to_user_id': legacy.get('in_reply_to_user_id_str'),
            'reply_to_username': legacy.get('in_reply_to_screen_name'),
            'retweeted_post_id': legacy.get('retweeted_status_id_str'),
            'quoted_post_id': legacy.get('quoted_status_id_str'),
            'original_post_creator_id': None,
            'original_post_creator_username': None,
            'relevant_to_master_list': False,
            'relevance_reason': None
        }

        # Extract original retweeted post data if available
        if extracted_data['is_retweet'] and 'retweeted_status_result' in tweet_data:
            rt_result = tweet_data['retweeted_status_result'].get('result', {})
            if 'legacy' in rt_result:
                rt_legacy = rt_result['legacy']
                rt_core = rt_result.get('core', {})
                rt_user = rt_core.get('user_results', {}).get('result', {}).get('legacy', {})

                # Set original post creator info in main data
                extracted_data['original_post_creator_id'] = rt_legacy.get('user_id_str', '')
                extracted_data['original_post_creator_username'] = rt_user.get('screen_name', '')

        # Extract original quoted post data if available
        if extracted_data['is_quote'] and 'quoted_status_result' in tweet_data:
            qt_result = tweet_data['quoted_status_result'].get('result', {})
            if 'legacy' in qt_result:
                qt_legacy = qt_result['legacy']
                qt_core = qt_result.get('core', {})
                qt_user = qt_core.get('user_results', {}).get('result', {}).get('legacy', {})

                # Set original post creator info in main data
                extracted_data['original_post_creator_id'] = qt_legacy.get('user_id_str', '')
                extracted_data['original_post_creator_username'] = qt_user.get('screen_name', '')

        # Check if post is relevant to master list
        relevance_reasons = []

        # 1. Check for mentions of master list users
        mentions = extract_mentions_from_text(extracted_data['text'])
        for mention in mentions:
            if mention in master_list['usernames']:
                relevance_reasons.append(f"mentions @{mention}")

        # 2. Check if retweet/quote is of a master list user
        if extracted_data['is_retweet'] and extracted_data['original_post_creator_id']:
            if (extracted_data['original_post_creator_id'] in master_list['user_ids'] or
                extracted_data['original_post_creator_username'].lower() in master_list['usernames']):
                relevance_reasons.append(f"retweets @{extracted_data['original_post_creator_username']}")

        if extracted_data['is_quote'] and extracted_data['original_post_creator_id']:
            if (extracted_data['original_post_creator_id'] in master_list['user_ids'] or
                extracted_data['original_post_creator_username'].lower() in master_list['usernames']):
                relevance_reasons.append(f"quotes @{extracted_data['original_post_creator_username']}")

        # 3. Check if reply is to a master list user
        if extracted_data['is_reply'] and extracted_data['reply_to_user_id']:
            if (extracted_data['reply_to_user_id'] in master_list['user_ids'] or
                extracted_data['reply_to_username'].lower() in master_list['usernames']):
                relevance_reasons.append(f"replies to @{extracted_data['reply_to_username']}")

        if relevance_reasons:
            extracted_data['relevant_to_master_list'] = True
            extracted_data['relevance_reason'] = "; ".join(relevance_reasons)

        return extracted_data

    except Exception as e:
        print(f"Error extracting post data: {e}")
        return None

def get_user_content(username, user_id, endpoint, days_back, master_list, max_content=1000):
    """Generic function to get user content (tweets or replies) from specified endpoint"""
    content = []
    cursor = None
    page = 0

    print(f"  Fetching {endpoint} for @{username} (ID: {user_id})...")

    while len(content) < max_content and page < 10:  # Limit pages
        page += 1

        params = f"user={user_id}&count=1000"
        if cursor:
            params += f"&cursor={cursor}"

        response = make_request(endpoint, params)

        if not response:
            break

        # Extract content from response
        found_content = False

        try:
            if 'result' in response and 'timeline' in response['result']:
                timeline = response['result']['timeline']
                if 'instructions' in timeline:
                    for instruction in timeline['instructions']:
                        if instruction.get('type') == 'TimelineAddEntries' and 'entries' in instruction:
                            for entry in instruction['entries']:
                                if 'tweet-' in entry.get('entryId', ''):
                                    entry_content = entry.get('content', {})
                                    if 'itemContent' in entry_content and 'tweet_results' in entry_content['itemContent']:
                                        content_data = entry_content['itemContent']['tweet_results'].get('result', {})

                                        # Check if within date range
                                        if is_post_within_days(content_data, days_back):
                                            extracted = extract_post_data(content_data, master_list)
                                            if extracted and extracted['relevant_to_master_list']:
                                                content.append(extracted)
                                                found_content = True
                                        else:
                                            # If we hit content outside date range, stop
                                            print(f"    Reached content outside date range for @{username}")
                                            return content
        except Exception as e:
            print(f"    Error parsing {endpoint} response for @{username}: {e}")
            break

        if not found_content:
            break

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

        if not cursor:
            break

        print(f"    Page {page}: Found {len([c for c in content if c]) if content else 0} relevant posts")

    print(f"  Total relevant {endpoint}: {len(content)} posts for @{username}")
    return content

def get_user_tweets(username, user_id, days_back, master_list, max_tweets=1000):
    """Get user's tweets using the /user-tweets endpoint"""
    return get_user_content(username, user_id, "/user-tweets", days_back, master_list, max_tweets)

def get_user_replies(username, user_id, days_back, master_list, max_replies=1000):
    """Get user's replies using the /user-replies-v2 endpoint"""
    return get_user_content(username, user_id, "/user-replies-v2", days_back, master_list, max_replies)

def fetch_non_member_interactions(non_member, days_back, master_list, community_id):
    """Fetch relevant interactions for a single non-member"""
    username = non_member.get('username', '')
    user_id = non_member.get('user_id', '')

    if not username and not user_id:
        print(f"  Skipping non-member with no username or user_id")
        return None

    if not user_id:
        print(f"  Skipping @{username} - no user_id available")
        return None

    print(f"\nProcessing non-member: @{username} (interactions with community: {non_member.get('interaction_counts', {}).get('total', 0)})")

    member_data = {
        'username': username,
        'user_id': user_id,
        'display_name': non_member.get('display_name', ''),
        'type': 'non_member',
        'posts': [],
        'replies': []
    }

    try:
        # Get user's regular posts/tweets that are relevant to master list
        tweets = get_user_tweets(username, user_id, days_back, master_list)
        for tweet in tweets:
            if tweet and tweet.get('relevant_to_master_list'):
                member_data['posts'].append(tweet)

        # Get user's replies that are relevant to master list
        replies = get_user_replies(username, user_id, days_back, master_list)
        for reply in replies:
            if reply and reply.get('relevant_to_master_list'):
                member_data['replies'].append(reply)

        print(f"  Found {len(member_data['posts'])} relevant posts and {len(member_data['replies'])} relevant replies")

        return member_data

    except Exception as e:
        print(f"  Error processing @{username}: {e}")
        return None

def analyze_interaction_types(non_members_interactions):
    """Analyze the types of interactions found"""
    stats = {
        'total_posts': 0,
        'total_replies': 0,
        'mentions': 0,
        'retweets': 0,
        'quotes': 0,
        'replies_to_master': 0,
        'community_posts': 0,
        'community_replies': 0
    }

    for member in non_members_interactions:
        stats['total_posts'] += len(member['posts'])
        stats['total_replies'] += len(member['replies'])

        # Analyze posts
        for post in member['posts']:
            if post.get('is_community_post'):
                stats['community_posts'] += 1

            if 'mentions' in post.get('relevance_reason', ''):
                stats['mentions'] += 1
            if post.get('is_retweet') and 'retweets' in post.get('relevance_reason', ''):
                stats['retweets'] += 1
            if post.get('is_quote') and 'quotes' in post.get('relevance_reason', ''):
                stats['quotes'] += 1

        # Analyze replies
        for reply in member['replies']:
            if reply.get('is_community_post'):
                stats['community_replies'] += 1

            if 'replies to' in reply.get('relevance_reason', ''):
                stats['replies_to_master'] += 1

    return stats

def save_non_member_interactions_data(community_id, non_members_interactions, raw_data_dir):
    """Save all non-member interactions to file"""

    # Analyze interaction types
    interaction_stats = analyze_interaction_types(non_members_interactions)

    # Create output data structure
    output_data = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_non_members_processed': len(non_members_interactions),
            'total_posts_fetched': interaction_stats['total_posts'],
            'total_replies_fetched': interaction_stats['total_replies'],
            'total_interactions': interaction_stats['total_posts'] + interaction_stats['total_replies'],
            'interaction_breakdown': {
                'mentions': interaction_stats['mentions'],
                'retweets': interaction_stats['retweets'],
                'quotes': interaction_stats['quotes'],
                'replies_to_master': interaction_stats['replies_to_master'],
                'community_posts': interaction_stats['community_posts'],
                'community_replies': interaction_stats['community_replies']
            }
        },
        'non_members_interactions': non_members_interactions
    }

    # Save to file
    filename = os.path.join(raw_data_dir, f"{community_id}_non_member_interactions.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\nNon-member interactions saved to: {filename}")
    print(f"Summary:")
    print(f"- Non-members processed: {len(non_members_interactions)}")
    print(f"- Total posts: {interaction_stats['total_posts']}")
    print(f"  * Mentions: {interaction_stats['mentions']}")
    print(f"  * Retweets: {interaction_stats['retweets']}")
    print(f"  * Quotes: {interaction_stats['quotes']}")
    print(f"  * Community posts: {interaction_stats['community_posts']}")
    print(f"- Total replies: {interaction_stats['total_replies']}")
    print(f"  * Replies to master list: {interaction_stats['replies_to_master']}")
    print(f"  * Community replies: {interaction_stats['community_replies']}")
    print(f"- Total interactions: {interaction_stats['total_posts'] + interaction_stats['total_replies']}")

    return filename, output_data['summary']

def main():
    """Main function - fetch non-member interactions and save to JSON files"""
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

        print(f"Non-Member Interactions Fetcher")
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

            # Load master list (members + non-members)
            master_data = load_members_and_non_members(community_id, raw_data_dir)
            if not master_data:
                print(f"Skipping community {community_id} - could not load member/non-member data")
                continue

            master_list, non_members = master_data

            if not non_members:
                print(f"No non-members found for community {community_id}")
                continue

            # Load checkpoint if exists
            processed_non_members = load_checkpoint(community_id, raw_data_dir)
            processed_usernames = get_processed_usernames(processed_non_members)

            # Filter out already processed non-members
            remaining_non_members = [nm for nm in non_members if nm.get('username', '').lower() not in processed_usernames]

            print(f"Total non-members: {len(non_members)}")
            print(f"Already processed: {len(processed_non_members)}")
            print(f"Remaining to process: {len(remaining_non_members)}")

            # Start with existing processed non-members
            non_members_interactions = processed_non_members.copy()

            # Process remaining non-members
            for j, non_member in enumerate(remaining_non_members):
                print(f"\nProcessing non-member {len(non_members_interactions)+1}/{len(non_members)}: {non_member.get('username', 'unknown')}")

                try:
                    member_data = fetch_non_member_interactions(non_member, days_back, master_list, community_id)
                    if member_data and (member_data['posts'] or member_data['replies']):
                        non_members_interactions.append(member_data)

                    # Save checkpoint every 5 non-members or on last non-member
                    if (j + 1) % 5 == 0 or j == len(remaining_non_members) - 1:
                        save_checkpoint(community_id, non_members_interactions, raw_data_dir)

                    # Small delay between non-members to be nice to the API
                    if j < len(remaining_non_members) - 1:
                        time.sleep(0.5)

                except Exception as e:
                    print(f"Error processing non-member @{non_member.get('username', 'unknown')}: {e}")
                    # Save checkpoint even on error to preserve progress
                    save_checkpoint(community_id, non_members_interactions, raw_data_dir)
                    # Continue with other non-members even if one fails
                    continue

            # Save interactions data
            if non_members_interactions:
                save_non_member_interactions_data(community_id, non_members_interactions, raw_data_dir)
                # Clean up checkpoint file after successful completion
                cleanup_checkpoint(community_id, raw_data_dir)
            else:
                print(f"No relevant interactions found for community {community_id}")

            # Delay between communities
            if i < len(community_ids) - 1:
                print(f"\nWaiting {community_delay} seconds before next community...")
                time.sleep(community_delay)

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'='*60}")
            print(f"NON-MEMBER INTERACTIONS FETCH COMPLETE")
            print(f"{'='*60}")
            print(f"Communities processed: {len(community_ids)}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

        print(f"\nOutput files saved in {raw_data_dir}:")
        print(f"- [community_id]_non_member_interactions.json")

    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"\nCheckpoint files are preserved in {raw_data_dir} for resuming:")
        print(f"- [community_id]_non_member_interactions_checkpoint.json")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

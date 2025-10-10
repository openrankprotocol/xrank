#!/usr/bin/env python3
"""
Internal Interactions Analyzer

This script analyzes internal interactions within a Twitter/X community by:
1. Loading members and non-members from existing JSON files
2. Fetching all community posts using the existing API infrastructure
3. Extracting interactions (mentions, replies, retweets, quotes) between community members
4. Saving the results to a new interactions file

Uses the same rate limiting and API infrastructure as x_communities.py
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

        # Log progress every 100 requests
        if request_count % 100 == 0:
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
                    backoff_time = 2 ** attempt
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
                    print(f"Error: HTTP {res.status}")
                    return None
            else:
                print(f"Error: HTTP {res.status}")
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

def load_community_users(community_id, raw_data_dir):
    """Load members and non-members from existing JSON files"""
    community_users = set()

    # Load members
    members_file = os.path.join(raw_data_dir, f"{community_id}_members.json")
    if os.path.exists(members_file):
        try:
            with open(members_file, 'r', encoding='utf-8') as f:
                members_data = json.load(f)

            members_count = 0
            if 'members' in members_data:
                for member in members_data['members']:
                    if member.get('username'):
                        community_users.add(member['username'].lower())
                        members_count += 1

            moderators_count = 0
            if 'moderators' in members_data:
                for moderator in members_data['moderators']:
                    if moderator.get('username'):
                        community_users.add(moderator['username'].lower())
                        moderators_count += 1

            print(f"✓ Loaded {members_count} members and {moderators_count} moderators")

        except Exception as e:
            print(f"❌ Error loading members file: {e}")
    else:
        print(f"❌ Members file not found: {members_file}")

    # Load non-members (external users who interact with community)
    non_members_file = os.path.join(raw_data_dir, f"{community_id}_non_members.json")
    if os.path.exists(non_members_file):
        try:
            with open(non_members_file, 'r', encoding='utf-8') as f:
                non_members_data = json.load(f)

            non_members_count = 0
            if 'external_users' in non_members_data:
                for user in non_members_data['external_users']:
                    if user.get('username'):
                        community_users.add(user['username'].lower())
                        non_members_count += 1

            print(f"✓ Loaded {non_members_count} non-members")

        except Exception as e:
            print(f"❌ Error loading non-members file: {e}")
    else:
        print(f"❌ Non-members file not found: {non_members_file}")

    print(f"📊 Total community users: {len(community_users)}")
    return community_users

def get_community_posts(community_id, cursor=None):
    """Fetch posts from a community"""
    endpoint = "/community-tweets"
    params = f"communityId={community_id}&searchType=Default&rankingMode=Recency&count=500"

    if cursor:
        params += f"&cursor={cursor}"

    return make_request(endpoint, params)

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

def extract_all_content_from_response(response):
    """Extract all posts, replies, likes, retweets from Twitter API response"""
    all_content = []

    if not response or not isinstance(response, dict):
        return all_content

    # Navigate through Twitter's complex response structure
    if 'result' in response and 'timeline' in response['result']:
        timeline = response['result']['timeline']
        if 'instructions' in timeline:
            for instruction in timeline['instructions']:
                # Handle different instruction types
                if instruction.get('type') == 'TimelineAddEntries' and 'entries' in instruction:
                    for entry in instruction['entries']:
                        content_item = extract_content_from_entry(entry)
                        if content_item:
                            all_content.append(content_item)

                elif instruction.get('type') == 'TimelineReplaceEntry' and 'entry' in instruction:
                    content_item = extract_content_from_entry(instruction['entry'])
                    if content_item:
                        all_content.append(content_item)

    return all_content

def extract_content_from_entry(entry):
    """Extract content from a timeline entry"""
    if not entry or 'content' not in entry:
        return None

    content = entry['content']
    entry_id = entry.get('entryId', '')

    # Skip cursor entries
    if 'cursor-' in entry_id:
        return None

    # Handle tweet entries
    if 'itemContent' in content:
        item = content['itemContent']
        if 'tweet_results' in item and 'result' in item['tweet_results']:
            tweet_data = item['tweet_results']['result']
            # Handle the extra 'tweet' layer in the structure
            if 'tweet' in tweet_data:
                tweet_data = tweet_data['tweet']
            return process_tweet_data(tweet_data, entry_id)

    # Handle conversation threads
    elif 'items' in content:
        thread_items = []
        for item in content['items']:
            if 'item' in item and 'itemContent' in item['item']:
                item_content = item['item']['itemContent']
                if 'tweet_results' in item_content and 'result' in item_content['tweet_results']:
                    tweet_data = item_content['tweet_results']['result']
                    # Handle the extra 'tweet' layer in the structure
                    if 'tweet' in tweet_data:
                        tweet_data = tweet_data['tweet']
                    processed_tweet = process_tweet_data(tweet_data, item.get('entryId', ''))
                    if processed_tweet:
                        thread_items.append(processed_tweet)

        if thread_items:
            return {
                'type': 'thread',
                'entry_id': entry_id,
                'items': thread_items
            }

    return None

def process_tweet_data(tweet_data, entry_id):
    """Process individual tweet data and extract basic information"""
    if not tweet_data:
        return None

    if 'legacy' not in tweet_data:
        return None

    legacy = tweet_data['legacy']
    core = tweet_data.get('core', {})
    user_data = core.get('user_results', {}).get('result', {}).get('legacy', {})
    tweet_id = legacy.get('id_str', '')

    # Extract basic tweet info
    processed_tweet = {
        'entry_id': entry_id,
        'tweet_id': tweet_id,
        'text': legacy.get('full_text', ''),
        'created_at': legacy.get('created_at', ''),
        'conversation_id': legacy.get('conversation_id_str', ''),

        # User info
        'user': {
            'screen_name': user_data.get('screen_name', ''),
            'name': user_data.get('name', ''),
        },

        # Content flags
        'is_retweet': legacy.get('retweeted_status_id_str') is not None,
        'is_reply': legacy.get('in_reply_to_status_id_str') is not None,
        'is_quote': legacy.get('quoted_status_id_str') is not None,

        # References
        'in_reply_to_status_id': legacy.get('in_reply_to_status_id_str'),
        'in_reply_to_screen_name': legacy.get('in_reply_to_screen_name'),
        'retweeted_status_id': legacy.get('retweeted_status_id_str'),
        'quoted_status_id': legacy.get('quoted_status_id_str'),
    }

    # Add retweeted/quoted user data if available
    if 'retweeted_status_result' in tweet_data:
        rt_result = tweet_data['retweeted_status_result']
        if 'result' in rt_result and 'core' in rt_result['result']:
            rt_user_data = rt_result['result']['core'].get('user_results', {}).get('result', {}).get('legacy', {})
            processed_tweet['retweeted_user'] = {
                'screen_name': rt_user_data.get('screen_name', ''),
                'name': rt_user_data.get('name', '')
            }
            if 'legacy' in rt_result['result']:
                processed_tweet['retweeted_text'] = rt_result['result']['legacy'].get('full_text', '')

    if 'quoted_status_result' in tweet_data:
        qt_result = tweet_data['quoted_status_result']
        if 'result' in qt_result and 'core' in qt_result['result']:
            qt_user_data = qt_result['result']['core'].get('user_results', {}).get('result', {}).get('legacy', {})
            processed_tweet['quoted_user'] = {
                'screen_name': qt_user_data.get('screen_name', ''),
                'name': qt_user_data.get('name', '')
            }
            if 'legacy' in qt_result['result']:
                processed_tweet['quoted_text'] = qt_result['result']['legacy'].get('full_text', '')

    return processed_tweet

def fetch_all_community_posts(community_id, config):
    """Fetch all posts from a community within specified days"""
    all_posts = []
    cursor = None
    page = 0
    days_back = config['data'].get('days_back_communities', 365)
    post_limit = config['data']['post_limit']

    print(f"Fetching posts from community: {community_id}")
    print(f"Date range: Last {days_back} days")
    print(f"Post limit: {post_limit if post_limit > 0 else 'unlimited'}")

    while True:
        page += 1
        print(f"Fetching page {page}...")

        response = get_community_posts(community_id, cursor)

        if not response:
            print("Failed to fetch posts")
            break

        # Extract all content using extraction method
        extracted_content = extract_all_content_from_response(response)

        if not extracted_content:
            print("No content extracted from response")
            break

        # Filter content by date
        valid_posts = []
        for item in extracted_content:
            # Handle different content types
            if item.get('type') == 'thread':
                # Process thread items
                thread_valid = []
                for thread_item in item.get('items', []):
                    if is_post_within_days(thread_item, days_back):
                        thread_valid.append(thread_item)
                if thread_valid:
                    item['items'] = thread_valid
                    valid_posts.append(item)
            else:
                # Regular post/tweet
                if is_post_within_days(item, days_back):
                    valid_posts.append(item)

        if not valid_posts:
            print(f"No posts within {days_back} days found, stopping...")
            break

        all_posts.extend(valid_posts)
        print(f"Collected {len(valid_posts)} items (total: {len(all_posts)})")

        # Check post limit
        if post_limit > 0 and len(all_posts) >= post_limit:
            print(f"Reached post limit of {post_limit}")
            all_posts = all_posts[:post_limit]
            break

        # Check for next page
        cursor = None
        if 'cursor' in response and 'bottom' in response['cursor']:
            cursor = response['cursor']['bottom']
        elif 'meta' in response and 'next_token' in response['meta']:
            cursor = response['meta']['next_token']
        elif 'next_cursor' in response:
            cursor = response['next_cursor']

        if not cursor:
            print("No more pages available")
            break

        # Safety limit
        if page >= 500:
            print("Reached page limit")
            break

    return all_posts

def extract_mentions_from_text(text):
    """Extract @mentions from tweet text"""
    if not text:
        return []

    # Find @mentions using regex
    mentions = re.findall(r'@(\w+)', text, re.IGNORECASE)
    return [mention.lower() for mention in mentions]

def extract_interactions_from_post(post_data, community_users_set):
    """Extract all interactions from a post (mentions, replies, retweets, quotes)"""
    interactions = []

    if not post_data:
        return interactions

    # Get basic info
    post_id = post_data.get('tweet_id', '')
    post_text = post_data.get('text', '')
    created_at = post_data.get('created_at', '')
    origin_user = post_data.get('user', {}).get('screen_name', '').lower()

    if not post_id or not origin_user:
        return interactions

    # Extract mentions from text
    mentions = extract_mentions_from_text(post_text)
    for mention in mentions:
        if mention in community_users_set and mention != origin_user:
            interactions.append({
                'type': 'mention',
                'origin_user': origin_user,
                'target_user': mention,
                'post_id': post_id,
                'post_type': 'tweet',
                'timestamp': created_at,
                'post_text': post_text[:200] + "..." if len(post_text) > 200 else post_text
            })

    # Check if it's a reply
    if post_data.get('in_reply_to_screen_name'):
        reply_to_user = post_data.get('in_reply_to_screen_name', '').lower()
        if reply_to_user in community_users_set and reply_to_user != origin_user:
            interactions.append({
                'type': 'reply',
                'origin_user': origin_user,
                'target_user': reply_to_user,
                'post_id': post_id,
                'post_type': 'reply',
                'timestamp': created_at,
                'reply_to_post_id': post_data.get('in_reply_to_status_id'),
                'post_text': post_text[:200] + "..." if len(post_text) > 200 else post_text
            })

    # Check if it's a retweet
    if post_data.get('is_retweet') and post_data.get('retweeted_user'):
        rt_username = post_data.get('retweeted_user', {}).get('screen_name', '').lower()
        if rt_username in community_users_set and rt_username != origin_user:
            rt_text = post_data.get('retweeted_text', '')
            interactions.append({
                'type': 'retweet',
                'origin_user': origin_user,
                'target_user': rt_username,
                'post_id': post_id,
                'post_type': 'retweet',
                'timestamp': created_at,
                'original_post_id': post_data.get('retweeted_status_id'),
                'post_text': rt_text[:200] + "..." if len(rt_text) > 200 else rt_text
            })

    # Check if it's a quote tweet
    if post_data.get('is_quote') and post_data.get('quoted_user'):
        qt_username = post_data.get('quoted_user', {}).get('screen_name', '').lower()
        if qt_username in community_users_set and qt_username != origin_user:
            qt_text = post_data.get('quoted_text', '')
            interactions.append({
                'type': 'quote',
                'origin_user': origin_user,
                'target_user': qt_username,
                'post_id': post_id,
                'post_type': 'quote',
                'timestamp': created_at,
                'quoted_post_id': post_data.get('quoted_status_id'),
                'post_text': post_text[:200] + "..." if len(post_text) > 200 else post_text,
                'quoted_text': qt_text[:200] + "..." if len(qt_text) > 200 else qt_text
            })

    return interactions

def save_community_interactions(interactions, community_id, raw_data_dir):
    """Save all community interactions to a file"""

    # Create summary statistics
    interaction_summary = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'total_interactions': len(interactions),
        'interaction_breakdown': {},
        'unique_origin_users': len(set(i['origin_user'] for i in interactions)),
        'unique_target_users': len(set(i['target_user'] for i in interactions))
    }

    # Count interaction types
    for interaction in interactions:
        itype = interaction['type']
        interaction_summary['interaction_breakdown'][itype] = interaction_summary['interaction_breakdown'].get(itype, 0) + 1

    # Create final data structure
    output_data = {
        'summary': interaction_summary,
        'interactions': interactions
    }

    # Save to file
    filename = os.path.join(raw_data_dir, f"{community_id}_internal_interactions.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Internal interactions saved to: {filename}")
    print(f"📊 Found {len(interactions)} total interactions:")
    for itype, count in interaction_summary['interaction_breakdown'].items():
        if count > 0:
            print(f"  - {itype.title()}s: {count}")

    print(f"👥 Unique users: {interaction_summary['unique_origin_users']} interacting, {interaction_summary['unique_target_users']} receiving")

    return filename

def main():
    """Main function to analyze internal community interactions"""
    global rate_limiter, request_count, start_time

    try:
        print("🏘️ INTERNAL COMMUNITY INTERACTIONS ANALYZER")
        print("=" * 60)

        # Load configuration
        config = load_config()
        if not config:
            return

        # Initialize rate limiter
        requests_per_second = config['rate_limiting']['requests_per_second']
        rate_limiter = RateLimiter(requests_per_second)

        # Reset counters
        request_count = 0
        start_time = None

        print(f"⚙️ Rate limiting: {requests_per_second} requests per second")

        community_ids = config['communities']['ids']
        raw_data_dir = config['output']['raw_data_dir']

        for i, community_id in enumerate(community_ids):
            print(f"\n{'='*60}")
            print(f"Processing community {i+1}/{len(community_ids)}: {community_id}")
            print(f"{'='*60}")

            # Step 1: Load community users from existing files
            print("Step 1: Loading community users from existing files...")
            community_users_set = load_community_users(community_id, raw_data_dir)

            if len(community_users_set) == 0:
                print("❌ No community users found. Make sure to run x_communities.py first.")
                continue

            # Step 2: Fetch community posts
            print("Step 2: Fetching community posts...")
            posts = fetch_all_community_posts(community_id, config)
            print(f"✅ Fetched {len(posts)} posts from community")

            # Step 3: Extract all interactions from community posts
            print("Step 3: Extracting interactions...")
            all_interactions = []

            processed_posts = 0
            for post in posts:
                if post.get('type') == 'thread':
                    # Process thread items
                    for thread_item in post.get('items', []):
                        interactions = extract_interactions_from_post(thread_item, community_users_set)
                        all_interactions.extend(interactions)
                        processed_posts += 1
                else:
                    # Regular post
                    interactions = extract_interactions_from_post(post, community_users_set)
                    all_interactions.extend(interactions)
                    processed_posts += 1

                # Progress update
                if processed_posts % 100 == 0:
                    print(f"  Processed {processed_posts} posts, found {len(all_interactions)} interactions so far")

            print(f"✅ Processed {processed_posts} posts total")

            # Step 4: Save community interactions
            print("Step 4: Saving results...")
            save_community_interactions(all_interactions, community_id, raw_data_dir)

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'='*60}")
            print(f"📈 ANALYSIS COMPLETE")
            print(f"{'='*60}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

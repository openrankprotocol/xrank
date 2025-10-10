#!/usr/bin/env python3
"""
Community Internal Interactions Analyzer (Final Production Version)

This script analyzes interactions within a Twitter/X community by:
1. Loading members and non-members for a particular community (master_list)
2. Getting user IDs for all users in the master list
3. Finding all personal posts and replies from `days_back` ago for every user
4. Filtering to keep only interactions between users in the master_list
5. Saving interaction data preserving: origin user, post ID, receiving user, interaction type

Uses correct endpoints:
- /user-tweets?user={user_id}&count=20 for user tweets
- /user-replies-v2?user={user_id}&count=20 for user replies

Rate limited to 10 requests per second to comply with API limits.

Output format:
- {community_id}_internal_interactions.json with detailed interaction data
- Interactions include: mentions, replies, retweets, quotes between community members
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
                    print(f"Error: HTTP {res.status} - {data.decode('utf-8')[:200]}")
                    return None
            else:
                print(f"Error: HTTP {res.status} - {data.decode('utf-8')[:200]}")
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

def get_user_id(username):
    """Get user ID from username using /user endpoint"""
    response = make_request("/user", f"username={username}")

    if response and 'result' in response:
        try:
            user_data = response['result']['data']['user']['result']
            user_id = user_data.get('rest_id')
            if user_id:
                return user_id
        except Exception as e:
            print(f"  Error extracting user ID for @{username}: {e}")

    return None

def load_master_list_with_ids(community_id, raw_data_dir):
    """Load members and non-members for a community and get their user IDs"""
    master_list = {}  # username -> user_id mapping
    usernames = set()

    # Load members
    members_file = os.path.join(raw_data_dir, f"{community_id}_members.json")
    if os.path.exists(members_file):
        try:
            with open(members_file, 'r', encoding='utf-8') as f:
                members_data = json.load(f)
                if 'members' in members_data:
                    for member in members_data['members']:
                        if 'username' in member and member['username']:
                            usernames.add(member['username'].lower())
                if 'moderators' in members_data:
                    for moderator in members_data['moderators']:
                        if 'username' in moderator and moderator['username']:
                            usernames.add(moderator['username'].lower())
            print(f"Loaded {len([m for m in members_data.get('members', []) if m.get('username')])} members")
            print(f"Loaded {len([m for m in members_data.get('moderators', []) if m.get('username')])} moderators")
        except Exception as e:
            print(f"Error loading members file: {e}")
    else:
        print(f"Members file not found: {members_file}")

    # Load non-members (external users who interact with community)
    non_members_file = os.path.join(raw_data_dir, f"{community_id}_non_members.json")
    if os.path.exists(non_members_file):
        try:
            with open(non_members_file, 'r', encoding='utf-8') as f:
                non_members_data = json.load(f)
                if 'external_users' in non_members_data:
                    for user in non_members_data['external_users']:
                        if 'username' in user and user['username']:
                            usernames.add(user['username'].lower())
            print(f"Loaded {len(non_members_data.get('external_users', []))} non-members")
        except Exception as e:
            print(f"Error loading non-members file: {e}")
    else:
        print(f"Non-members file not found: {non_members_file}")

    print(f"Total unique usernames: {len(usernames)}")

    # Get user IDs for all usernames
    print(f"Fetching user IDs for all users...")
    failed_users = []

    for i, username in enumerate(usernames):
        if (i + 1) % 50 == 0 or i == 0:
            print(f"Progress: {i+1}/{len(usernames)} - Getting ID for @{username}")

        user_id = get_user_id(username)
        if user_id:
            master_list[username] = user_id
        else:
            failed_users.append(username)

    print(f"Successfully got user IDs for {len(master_list)}/{len(usernames)} users")
    if failed_users:
        print(f"Failed to get IDs for {len(failed_users)} users: {failed_users[:10]}{'...' if len(failed_users) > 10 else ''}")

    return master_list

def is_post_within_days(post_data, days_back):
    """Check if post is within the specified days back"""
    try:
        if 'created_at' in post_data:
            created_at = post_data['created_at']
        elif 'tweet' in post_data and 'created_at' in post_data['tweet']:
            created_at = post_data['tweet']['created_at']
        elif 'legacy' in post_data and 'created_at' in post_data['legacy']:
            created_at = post_data['legacy']['created_at']
        else:
            return False

        # Parse Twitter's date format
        post_date = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
        cutoff_date = datetime.now(post_date.tzinfo) - timedelta(days=days_back)

        return post_date >= cutoff_date
    except Exception as e:
        return False

def extract_mentions_from_text(text):
    """Extract @mentions from tweet text"""
    if not text:
        return []

    # Find @mentions using regex
    mentions = re.findall(r'@(\w+)', text, re.IGNORECASE)
    return [mention.lower() for mention in mentions]

def get_user_content(username, user_id, endpoint, days_back, max_content=1000):
    """Generic function to get user content (tweets or replies) from specified endpoint"""
    content = []
    cursor = None
    page = 0

    while len(content) < max_content and page < 10:  # Limit pages
        page += 1

        params = f"user={user_id}&count=100"
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
                                            content.append(content_data)
                                            found_content = True
                                        else:
                                            # If we hit content outside date range, stop
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

    return content

def get_user_tweets(username, user_id, days_back, max_tweets=1000):
    """Get user's tweets using the correct endpoint"""
    return get_user_content(username, user_id, "/user-tweets", days_back, max_tweets)

def get_user_replies(username, user_id, days_back, max_replies=1000):
    """Get user's replies using the correct endpoint"""
    return get_user_content(username, user_id, "/user-replies-v2", days_back, max_replies)

def analyze_post_interactions(post_data, master_list, origin_username, post_type="tweet"):
    """Analyze a post/tweet/reply for interactions with users in master_list"""
    interactions = []

    try:
        # Get post text for mention analysis
        post_text = ""
        if 'tweet' in post_data and 'legacy' in post_data['tweet']:
            post_text = post_data['tweet']['legacy'].get('full_text', '')
        elif 'legacy' in post_data:
            post_text = post_data['legacy'].get('full_text', '')

        # Get post ID
        post_id = None
        if 'tweet' in post_data and 'legacy' in post_data['tweet']:
            post_id = post_data['tweet']['legacy'].get('id_str')
        elif 'legacy' in post_data:
            post_id = post_data['legacy'].get('id_str')
        elif 'rest_id' in post_data:
            post_id = post_data['rest_id']

        if not post_id:
            return interactions

        # Check for mentions in the text
        mentions = extract_mentions_from_text(post_text)
        for mention in mentions:
            if mention in master_list and mention != origin_username.lower():
                interactions.append({
                    'type': 'mention',
                    'origin_user': origin_username,
                    'receiving_user': mention,
                    'post_id': post_id,
                    'post_text': post_text[:200] + "..." if len(post_text) > 200 else post_text,
                    'post_type': post_type,
                    'timestamp': datetime.now().isoformat()
                })

        # Check if it's a reply to another user in master_list
        if 'tweet' in post_data and 'legacy' in post_data['tweet']:
            legacy_data = post_data['tweet']['legacy']
        elif 'legacy' in post_data:
            legacy_data = post_data['legacy']
        else:
            legacy_data = {}

        if 'in_reply_to_screen_name' in legacy_data:
            reply_to_user = legacy_data['in_reply_to_screen_name'].lower()
            if reply_to_user in master_list and reply_to_user != origin_username.lower():
                reply_to_tweet_id = legacy_data.get('in_reply_to_status_id_str')
                interactions.append({
                    'type': 'reply',
                    'origin_user': origin_username,
                    'receiving_user': reply_to_user,
                    'post_id': post_id,
                    'reply_to_post_id': reply_to_tweet_id,
                    'post_text': post_text[:200] + "..." if len(post_text) > 200 else post_text,
                    'post_type': post_type,
                    'timestamp': datetime.now().isoformat()
                })

        # Check if it's a retweet of another user in master_list
        if 'tweet' in post_data and 'legacy' in post_data['tweet']:
            if 'retweeted_status_result' in post_data['tweet']:
                rt_result = post_data['tweet']['retweeted_status_result']
                if 'result' in rt_result and 'core' in rt_result['result']:
                    rt_user_data = rt_result['result']['core'].get('user_results', {}).get('result', {})
                    if 'legacy' in rt_user_data:
                        rt_username = rt_user_data['legacy'].get('screen_name', '').lower()
                        if rt_username in master_list and rt_username != origin_username.lower():
                            rt_tweet_id = rt_result['result']['legacy'].get('id_str')
                            rt_text = rt_result['result']['legacy'].get('full_text', '')
                            interactions.append({
                                'type': 'retweet',
                                'origin_user': origin_username,
                                'receiving_user': rt_username,
                                'post_id': post_id,
                                'original_post_id': rt_tweet_id,
                                'post_text': rt_text[:200] + "..." if len(rt_text) > 200 else rt_text,
                                'post_type': post_type,
                                'timestamp': datetime.now().isoformat()
                            })

        # Check if it's a quote tweet of another user in master_list
        if 'tweet' in post_data and 'quoted_status_result' in post_data['tweet']:
            qt_result = post_data['tweet']['quoted_status_result']
            if 'result' in qt_result and 'core' in qt_result['result']:
                qt_user_data = qt_result['result']['core'].get('user_results', {}).get('result', {})
                if 'legacy' in qt_user_data:
                    qt_username = qt_user_data['legacy'].get('screen_name', '').lower()
                    if qt_username in master_list and qt_username != origin_username.lower():
                        qt_tweet_id = qt_result['result']['legacy'].get('id_str')
                        qt_text = qt_result['result']['legacy'].get('full_text', '')
                        interactions.append({
                            'type': 'quote',
                            'origin_user': origin_username,
                            'receiving_user': qt_username,
                            'post_id': post_id,
                            'quoted_post_id': qt_tweet_id,
                            'post_text': post_text[:200] + "..." if len(post_text) > 200 else post_text,
                            'quoted_text': qt_text[:200] + "..." if len(qt_text) > 200 else qt_text,
                            'post_type': post_type,
                            'timestamp': datetime.now().isoformat()
                        })

    except Exception as e:
        print(f"    Error analyzing post {post_id}: {e}")

    return interactions

def analyze_user_interactions(username, user_id, master_list, days_back):
    """Analyze all interactions for a specific user"""
    all_interactions = []

    # Get user's tweets
    tweets = get_user_tweets(username, user_id, days_back)

    # Analyze each tweet for interactions
    for tweet in tweets:
        interactions = analyze_post_interactions(tweet, master_list, username, "tweet")
        all_interactions.extend(interactions)

    # Get user's replies
    replies = get_user_replies(username, user_id, days_back)

    # Analyze each reply for interactions
    for reply in replies:
        interactions = analyze_post_interactions(reply, master_list, username, "reply")
        all_interactions.extend(interactions)

    return {
        'tweets_analyzed': len(tweets),
        'replies_analyzed': len(replies),
        'interactions_found': len(all_interactions),
        'interactions': all_interactions
    }

def save_interactions_data(community_id, all_results, master_list, raw_data_dir):
    """Save all interaction data to file"""

    # Collect all interactions
    all_interactions = []
    for result in all_results.values():
        all_interactions.extend(result['interactions'])

    # Organize interactions by type
    interactions_by_type = {
        'mention': [],
        'reply': [],
        'retweet': [],
        'quote': []
    }

    for interaction in all_interactions:
        interaction_type = interaction['type']
        if interaction_type in interactions_by_type:
            interactions_by_type[interaction_type].append(interaction)

    # Create summary statistics
    total_tweets = sum(result['tweets_analyzed'] for result in all_results.values())
    total_replies = sum(result['replies_analyzed'] for result in all_results.values())

    summary = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'analysis_summary': {
            'total_users_analyzed': len(master_list),
            'total_tweets_analyzed': total_tweets,
            'total_replies_analyzed': total_replies,
            'total_posts_analyzed': total_tweets + total_replies
        },
        'interactions_summary': {
            'total_interactions': len(all_interactions),
            'interaction_breakdown': {
                interaction_type: len(interactions)
                for interaction_type, interactions in interactions_by_type.items()
            },
            'unique_users_interacting': len(set(i['origin_user'] for i in all_interactions)),
            'unique_users_receiving': len(set(i['receiving_user'] for i in all_interactions))
        }
    }

    # Create final data structure
    output_data = {
        'summary': summary,
        'interactions': {
            'mentions': interactions_by_type['mention'],
            'replies': interactions_by_type['reply'],
            'retweets': interactions_by_type['retweet'],
            'quotes': interactions_by_type['quote']
        },
        'user_analysis_details': {
            username: {
                'tweets_analyzed': result['tweets_analyzed'],
                'replies_analyzed': result['replies_analyzed'],
                'interactions_found': result['interactions_found']
            }
            for username, result in all_results.items()
        }
    }

    # Save to file
    filename = os.path.join(raw_data_dir, f"{community_id}_internal_interactions.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\nInternal interactions saved to: {filename}")
    return filename, summary

def main():
    """Main function to analyze internal community interactions"""
    global rate_limiter, request_count, start_time

    try:
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

        print(f"Community Internal Interactions Analyzer")
        print(f"Rate limiting: {requests_per_second} requests per second")
        print(f"=" * 70)

        community_ids = config['communities']['ids']
        raw_data_dir = config['output']['raw_data_dir']
        days_back_communities = config['data']['days_back_communities']

        for i, community_id in enumerate(community_ids):
            print(f"\n{'='*70}")
            print(f"Processing community {i+1}/{len(community_ids)}: {community_id}")
            print(f"{'='*70}")

            # Step 1: Load master list (members + non-members) and get user IDs
            print("Step 1: Loading master list and fetching user IDs...")
            master_list = load_master_list_with_ids(community_id, raw_data_dir)

            if len(master_list) == 0:
                print("No users found with valid IDs. Skipping community.")
                continue

            # Step 2: Analyze interactions for each user
            print(f"\nStep 2: Analyzing interactions for {len(master_list)} users...")
            print(f"Looking for content from the last {days_back_communities} days")

            all_results = {}
            processed_users = 0

            for username, user_id in master_list.items():
                processed_users += 1

                if processed_users % 25 == 0 or processed_users == 1:
                    print(f"\nProgress: {processed_users}/{len(master_list)} users - @{username}")

                try:
                    user_result = analyze_user_interactions(username, user_id, master_list, days_back_communities)
                    all_results[username] = user_result

                    print(f"  Found {user_result['interactions_found']} interactions")

                except Exception as e:
                    print(f"  Error analyzing user @{username}: {e}")
                    all_results[username] = {
                        'tweets_analyzed': 0,
                        'replies_analyzed': 0,
                        'interactions_found': 0,
                        'interactions': []
                    }

                # Small delay between users every 20 users
                if processed_users % 20 == 0:
                    time.sleep(2)

            # Step 3: Save results
            print(f"\nStep 3: Saving interaction data...")
            filename, summary = save_interactions_data(community_id, all_results, master_list, raw_data_dir)

            # Summary for this community
            print(f"\n" + "="*50)
            print(f"Community {community_id} Analysis Complete")
            print(f"="*50)
            print(f"Users with valid IDs: {len(master_list)}")
            print(f"Users analyzed: {processed_users}")
            print(f"Total posts analyzed: {summary['analysis_summary']['total_posts_analyzed']}")
            print(f"  - Tweets: {summary['analysis_summary']['total_tweets_analyzed']}")
            print(f"  - Replies: {summary['analysis_summary']['total_replies_analyzed']}")
            print(f"Date range: Last {days_back_communities} days")
            print(f"\nTotal interactions found: {summary['interactions_summary']['total_interactions']}")

            for itype, count in summary['interactions_summary']['interaction_breakdown'].items():
                if count > 0:
                    print(f"  - {itype.title()}s: {count}")

            print(f"\nUnique users interacting: {summary['interactions_summary']['unique_users_interacting']}")
            print(f"Unique users receiving interactions: {summary['interactions_summary']['unique_users_receiving']}")

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'='*70}")
            print(f"ANALYSIS COMPLETE")
            print(f"{'='*70}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
            print(f"- Average rate: {avg_rate:.2f} requests/second")
            print(f"\nFiles saved in {raw_data_dir}:")
            print(f"- {community_id}_internal_interactions.json (detailed interaction data)")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

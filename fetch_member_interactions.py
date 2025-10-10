#!/usr/bin/env python3
"""
Community Members Interactions Fetcher

This script fetches all interactions from community members by:
1. Loading the list of members from fetch_members.py output
2. Fetching all posts/tweets of these members going back days_back time
3. Including regular posts, community posts, and replies to any other posts
4. Saving the data to raw/[community_id]_members_interactions.json

Uses endpoints:
- /user-tweets for user timeline posts (includes original posts, retweets, quotes)
- /user-replies-v2 for user replies to other posts

Enhanced Data Structure:
- For retweets: Preserves original_post_creator_id and original_post_creator_username
- For quotes: Preserves original_post_creator_id and original_post_creator_username
- For replies: Preserves reply_to_user_id and reply_to_username
- Preserves community_id and is_community_post flag when posts are made to communities
- No separate community interaction classification - community context preserved in post data

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

def save_checkpoint(community_id, processed_members, raw_data_dir):
    """Save checkpoint data to resume processing later"""
    checkpoint_file = os.path.join(raw_data_dir, f"{community_id}_member_interactions_checkpoint.json")
    checkpoint_data = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'processed_members': processed_members,
        'total_processed': len(processed_members)
    }

    os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)

    print(f"Checkpoint saved: {len(processed_members)} members processed")

def load_checkpoint(community_id, raw_data_dir):
    """Load checkpoint data to resume processing"""
    checkpoint_file = os.path.join(raw_data_dir, f"{community_id}_member_interactions_checkpoint.json")

    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)

            print(f"Found checkpoint: {len(checkpoint_data.get('processed_members', []))} members already processed")
            return checkpoint_data.get('processed_members', [])
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
            return []

    return []

def cleanup_checkpoint(community_id, raw_data_dir):
    """Remove checkpoint file after successful completion"""
    checkpoint_file = os.path.join(raw_data_dir, f"{community_id}_member_interactions_checkpoint.json")

    if os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
            print(f"Checkpoint file cleaned up")
        except Exception as e:
            print(f"Warning: Could not remove checkpoint file: {e}")

def get_processed_usernames(processed_members):
    """Extract usernames from processed members list"""
    return {member.get('username', '').lower() for member in processed_members if member.get('username')}


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

        # Extract all members and moderators with their IDs
        all_members = []

        # Add regular members
        for member in members_data.get('members', []):
            if member.get('username') and member.get('user_id'):
                all_members.append({
                    'username': member['username'],
                    'user_id': member['user_id'],
                    'display_name': member.get('display_name', ''),
                    'role': 'member'
                })

        # Add moderators
        for moderator in members_data.get('moderators', []):
            if moderator.get('username') and moderator.get('user_id'):
                all_members.append({
                    'username': moderator['username'],
                    'user_id': moderator['user_id'],
                    'display_name': moderator.get('display_name', ''),
                    'role': 'moderator'
                })

        print(f"Loaded {len(all_members)} community members from {filename}")
        return all_members

    except Exception as e:
        print(f"Error loading members file {filename}: {e}")
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

def extract_post_data(post_data):
    """Extract relevant data from a post/tweet"""
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

        # Look for community information in various places in the response
        if 'community' in tweet_data:
            community_info = tweet_data['community']
            if isinstance(community_info, dict) and 'id_str' in community_info:
                community_id = community_info['id_str']
                is_community_post = True

        # Also check in legacy data for community context
        if not community_id and 'conversation_control' in legacy:
            conv_control = legacy['conversation_control']
            if isinstance(conv_control, dict) and 'policy' in conv_control:
                policy = conv_control['policy']
                if isinstance(policy, dict) and 'can_reply' in policy:
                    # This might indicate community post context, but we need community_id
                    pass

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
            'original_post_creator_id': None,  # Will be populated for retweets/quotes
            'original_post_creator_username': None  # Will be populated for retweets/quotes
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

                extracted_data['retweeted_post'] = {
                    'post_id': rt_legacy.get('id_str', ''),
                    'text': rt_legacy.get('full_text', ''),
                    'created_at': rt_legacy.get('created_at', ''),
                    'user_id': rt_legacy.get('user_id_str', ''),
                    'username': rt_user.get('screen_name', ''),
                    'user_display_name': rt_user.get('name', '')
                }

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

                extracted_data['quoted_post'] = {
                    'post_id': qt_legacy.get('id_str', ''),
                    'text': qt_legacy.get('full_text', ''),
                    'created_at': qt_legacy.get('created_at', ''),
                    'user_id': qt_legacy.get('user_id_str', ''),
                    'username': qt_user.get('screen_name', ''),
                    'user_display_name': qt_user.get('name', '')
                }

        return extracted_data

    except Exception as e:
        print(f"Error extracting post data: {e}")
        return None

def get_user_content(username, user_id, endpoint, days_back, max_content=1000):
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
                                            extracted = extract_post_data(content_data)
                                            if extracted:
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

        print(f"    Page {page}: Found {len([c for c in content if c]) if content else 0} posts")

    print(f"  Total {endpoint}: {len(content)} posts for @{username}")
    return content

def get_user_tweets(username, user_id, days_back, max_tweets=1000):
    """Get user's tweets using the /user-tweets endpoint"""
    return get_user_content(username, user_id, "/user-tweets", days_back, max_tweets)

def get_user_replies(username, user_id, days_back, max_replies=1000):
    """Get user's replies using the /user-replies-v2 endpoint"""
    return get_user_content(username, user_id, "/user-replies-v2", days_back, max_replies)

def fetch_member_interactions(member, days_back, community_id):
    """Fetch all interactions for a single member"""
    username = member['username']
    user_id = member['user_id']

    print(f"\nProcessing member: @{username} ({member['role']})")

    member_data = {
        'username': username,
        'user_id': user_id,
        'display_name': member['display_name'],
        'role': member['role'],
        'posts': [],
        'replies': []
    }

    # Get user's regular posts/tweets
    tweets = get_user_tweets(username, user_id, days_back)
    for tweet in tweets:
        if tweet:
            member_data['posts'].append(tweet)

    # Get user's replies to other posts
    replies = get_user_replies(username, user_id, days_back)
    for reply in replies:
        if reply:
            member_data['replies'].append(reply)

    print(f"  Found {len(member_data['posts'])} posts and {len(member_data['replies'])} replies")

    return member_data

def analyze_interaction_types(members_interactions):
    """Analyze the types of interactions found"""
    stats = {
        'total_posts': 0,
        'total_replies': 0,
        'retweets': 0,
        'quotes': 0,
        'original_posts': 0,
        'reply_interactions': 0,
        'community_posts': 0,
        'community_replies': 0
    }

    for member in members_interactions:
        stats['total_posts'] += len(member['posts'])
        stats['total_replies'] += len(member['replies'])

        # Analyze posts
        for post in member['posts']:
            if post.get('is_community_post'):
                stats['community_posts'] += 1

            if post.get('is_retweet'):
                stats['retweets'] += 1
            elif post.get('is_quote'):
                stats['quotes'] += 1
            else:
                stats['original_posts'] += 1

        # Analyze replies
        for reply in member['replies']:
            if reply.get('is_community_post'):
                stats['community_replies'] += 1

            if reply.get('reply_to_post_id'):
                stats['reply_interactions'] += 1

    return stats

def save_interactions_data(community_id, members_interactions, raw_data_dir):
    """Save all member interactions to file"""

    # Analyze interaction types
    interaction_stats = analyze_interaction_types(members_interactions)

    # Create output data structure
    output_data = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_members_processed': len(members_interactions),
            'total_posts_fetched': interaction_stats['total_posts'],
            'total_replies_fetched': interaction_stats['total_replies'],
            'total_interactions': interaction_stats['total_posts'] + interaction_stats['total_replies'],
            'interaction_breakdown': {
                'original_posts': interaction_stats['original_posts'],
                'retweets': interaction_stats['retweets'],
                'quotes': interaction_stats['quotes'],
                'replies': interaction_stats['reply_interactions'],
                'community_posts': interaction_stats['community_posts'],
                'community_replies': interaction_stats['community_replies']
            }
        },
        'members_interactions': members_interactions
    }

    # Save to file
    filename = os.path.join(raw_data_dir, f"{community_id}_members_interactions.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\nMember interactions saved to: {filename}")
    print(f"Summary:")
    print(f"- Members processed: {len(members_interactions)}")
    print(f"- Total posts: {interaction_stats['total_posts']}")
    print(f"  * Original posts: {interaction_stats['original_posts']}")
    print(f"  * Retweets: {interaction_stats['retweets']}")
    print(f"  * Quotes: {interaction_stats['quotes']}")
    print(f"  * Community posts: {interaction_stats['community_posts']}")
    print(f"- Total replies: {interaction_stats['total_replies']}")
    print(f"  * Community replies: {interaction_stats['community_replies']}")
    print(f"- Total interactions: {interaction_stats['total_posts'] + interaction_stats['total_replies']}")

    return filename, output_data['summary']

def main():
    """Main function - fetch community member interactions and save to JSON files"""
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

        print(f"Community Members Interactions Fetcher")
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

            # Load members list for this community
            members = load_members_list(community_id, raw_data_dir)
            if not members:
                print(f"Skipping community {community_id} - no members found")
                continue

            # Load checkpoint if exists
            processed_members = load_checkpoint(community_id, raw_data_dir)
            processed_usernames = get_processed_usernames(processed_members)

            # Filter out already processed members
            remaining_members = [m for m in members if m.get('username', '').lower() not in processed_usernames]

            print(f"Total members: {len(members)}")
            print(f"Already processed: {len(processed_members)}")
            print(f"Remaining to process: {len(remaining_members)}")

            # Start with existing processed members
            members_interactions = processed_members.copy()

            # Process remaining members
            for j, member in enumerate(remaining_members):
                print(f"\nProcessing member {len(members_interactions)+1}/{len(members)}: {member['username']}")

                try:
                    member_data = fetch_member_interactions(member, days_back, community_id)
                    if member_data:  # Only add if we got valid data
                        members_interactions.append(member_data)

                    # Save checkpoint every 10 members or on last member
                    if (j + 1) % 10 == 0 or j == len(remaining_members) - 1:
                        save_checkpoint(community_id, members_interactions, raw_data_dir)

                    # Small delay between members to be nice to the API
                    if j < len(remaining_members) - 1:
                        time.sleep(0.5)

                except Exception as e:
                    print(f"Error processing member @{member['username']}: {e}")
                    # Save checkpoint even on error to preserve progress
                    save_checkpoint(community_id, members_interactions, raw_data_dir)
                    # Continue with other members even if one fails
                    continue

            # Save interactions data
            if members_interactions:
                save_interactions_data(community_id, members_interactions, raw_data_dir)
                # Clean up checkpoint file after successful completion
                cleanup_checkpoint(community_id, raw_data_dir)
            else:
                print(f"No interactions data collected for community {community_id}")

            # Delay between communities
            if i < len(community_ids) - 1:
                print(f"\nWaiting {community_delay} seconds before next community...")
                time.sleep(community_delay)

        # Final summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\n{'='*60}")
            print(f"INTERACTIONS FETCH COMPLETE")
            print(f"{'='*60}")
            print(f"Communities processed: {len(community_ids)}")
            print(f"API Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

        print(f"\nOutput files saved in {raw_data_dir}:")
        print(f"- [community_id]_members_interactions.json")

    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"\nCheckpoint files are preserved in {raw_data_dir} for resuming:")
        print(f"- [community_id]_member_interactions_checkpoint.json")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

import http.client
import json
import os
import toml
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import threading

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
        with open('config.toml', 'r') as f:
            return toml.load(f)
    except FileNotFoundError:
        print("config.toml not found, using default values")
        return {
            'data': {'days_back': 730, 'post_limit': 10000},
            'communities': {'ids': ["1601841656147345410"]},
            'output': {'raw_data_dir': "./raw"},
            'rate_limiting': {'request_delay': 1.0, 'community_delay': 2.0}
        }

def get_api_key():
    """Get API key from .env file or environment, removing any quotes"""
    api_key = os.getenv('RAPIDAPI_KEY')

    if not api_key:
        try:
            with open('.env', 'r') as f:
                for line in f:
                    if line.startswith('RAPIDAPI_KEY='):
                        api_key = line.split('=', 1)[1].strip()
                        break
        except FileNotFoundError:
            pass

    if not api_key:
        raise ValueError("RAPIDAPI_KEY not found in environment variables or .env file")

    # CRITICAL FIX: Remove any surrounding quotes that cause authentication to fail
    api_key = api_key.strip('"\'')

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

def is_post_within_days(post_data, days_back):
    """Check if post is within the specified days back"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_back)

        if 'created_at' in post_data:
            # Parse Twitter's date format
            post_date = datetime.strptime(
                post_data['created_at'],
                "%a %b %d %H:%M:%S %z %Y"
            ).replace(tzinfo=None)
            return post_date >= cutoff_date
        elif 'timestamp' in post_data:
            post_date = datetime.fromtimestamp(int(post_data['timestamp']))
            return post_date >= cutoff_date
        else:
            return True  # Include if no date info

    except Exception as e:
        print(f"Error parsing date: {str(e)}")
        return True

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

def get_users_who_interacted(tweet_id):
    """Get users who retweeted, liked, replied to, or quoted a tweet"""
    interactions = {
        'retweeted_by': [],
        'liked_by': [],
        'replied_by': [],
        'quoted_by': []
    }

    # Get users who retweeted
    try:
        retweet_response = make_request("/retweets", f"pid={tweet_id}&count=200")
        if retweet_response:
            # Handle timeline structure for retweets
            timeline_data = None
            if 'result' in retweet_response and 'timeline' in retweet_response['result']:
                timeline_data = retweet_response['result']['timeline']
            elif 'data' in retweet_response and 'retweeters_timeline' in retweet_response['data']:
                timeline_data = retweet_response['data']['retweeters_timeline']['timeline']

            if timeline_data and 'instructions' in timeline_data:
                for instruction in timeline_data['instructions']:
                    if 'entries' in instruction:
                        for entry in instruction['entries']:
                            if 'user-' in entry.get('entryId', ''):
                                # Extract user from timeline entry
                                content = entry.get('content', {})
                                if 'itemContent' in content and 'user_results' in content['itemContent']:
                                    user_data = content['itemContent']['user_results'].get('result', {})
                                    # CRITICAL FIX: Try multiple paths for screen_name
                                    username = None

                                    # Path 1: core.screen_name (newer API responses)
                                    if 'core' in user_data:
                                        username = user_data['core'].get('screen_name')

                                    # Path 2: legacy.screen_name (older API responses)
                                    if not username and 'legacy' in user_data:
                                        username = user_data['legacy'].get('screen_name')

                                    if username:
                                        interactions['retweeted_by'].append(username)
    except Exception as e:
        print(f"Error fetching retweeters for {tweet_id}: {e}")

    # Get users who liked
    try:
        likes_response = make_request("/likes", f"pid={tweet_id}&count=200")
        if likes_response:
            # Handle timeline structure for likes
            if 'result' in likes_response and 'timeline' in likes_response['result']:
                timeline = likes_response['result']['timeline']
                if 'instructions' in timeline:
                    for instruction in timeline['instructions']:
                        if instruction.get('type') == 'TimelineAddEntries' and 'entries' in instruction:
                            for entry in instruction['entries']:
                                entry_id = entry.get('entryId', '')
                                if entry_id.startswith('user-'):
                                    # Extract user from timeline entry
                                    content = entry.get('content', {})
                                    if 'itemContent' in content and content['itemContent'].get('itemType') == 'TimelineUser':
                                        user_results = content['itemContent'].get('user_results', {})
                                        if 'result' in user_results:
                                            user_data = user_results['result']
                                            # CRITICAL FIX: Try multiple paths for screen_name
                                            username = None

                                            # Path 1: core.screen_name
                                            if 'core' in user_data:
                                                username = user_data['core'].get('screen_name')

                                            # Path 2: legacy.screen_name
                                            if not username and 'legacy' in user_data:
                                                username = user_data['legacy'].get('screen_name')

                                            if username:
                                                interactions['liked_by'].append(username)
    except Exception as e:
        print(f"Error fetching likes for {tweet_id}: {e}")

    # Get replies (comments)
    try:
        replies_response = make_request("/comments-v2", f"pid={tweet_id}&rankingMode=Relevance&count=100")
        if replies_response:
            # Handle threaded conversation structure
            if 'result' in replies_response and 'data' in replies_response['result']:
                data = replies_response['result']['data']
                if 'threaded_conversation_with_injections_v2' in data:
                    instructions = data['threaded_conversation_with_injections_v2'].get('instructions', [])

                    for instruction in instructions:
                        if instruction.get('type') == 'TimelineAddEntries' and 'entries' in instruction:
                            for entry in instruction['entries']:
                                entry_id = entry.get('entryId', '')

                                # Handle individual tweet entries
                                if entry_id.startswith('tweet-') and entry_id != f'tweet-{tweet_id}':
                                    content = entry.get('content', {})
                                    if 'itemContent' in content and 'tweet_results' in content['itemContent']:
                                        tweet_data = content['itemContent']['tweet_results'].get('result', {})
                                        if 'core' in tweet_data and 'user_results' in tweet_data['core']:
                                            user_data = tweet_data['core']['user_results'].get('result', {})
                                            # CRITICAL FIX: Try multiple paths for screen_name
                                            username = None

                                            # Path 1: core.screen_name
                                            if 'core' in user_data:
                                                username = user_data['core'].get('screen_name')

                                            # Path 2: legacy.screen_name
                                            if not username and 'legacy' in user_data:
                                                username = user_data['legacy'].get('screen_name')

                                            if username:
                                                interactions['replied_by'].append(username)

                                # Handle conversation threads
                                elif entry_id.startswith('conversationthread-'):
                                    content = entry.get('content', {})
                                    if 'items' in content:
                                        for item in content['items']:
                                            item_content = item.get('item', {}).get('itemContent', {})
                                            if 'tweet_results' in item_content:
                                                tweet_data = item_content['tweet_results'].get('result', {})
                                                if 'core' in tweet_data and 'user_results' in tweet_data['core']:
                                                    user_data = tweet_data['core']['user_results'].get('result', {})
                                                    # CRITICAL FIX: Try multiple paths for screen_name
                                                    username = None

                                                    # Path 1: core.screen_name
                                                    if 'core' in user_data:
                                                        username = user_data['core'].get('screen_name')

                                                    # Path 2: legacy.screen_name
                                                    if not username and 'legacy' in user_data:
                                                        username = user_data['legacy'].get('screen_name')

                                                    if username:
                                                        interactions['replied_by'].append(username)
    except Exception as e:
        print(f"Error fetching replies for {tweet_id}: {e}")

    # Get quotes
    try:
        quotes_response = make_request("/quotes", f"pid={tweet_id}&count=200")
        if quotes_response:
            # Handle timeline structure for quotes
            timeline_data = None
            if 'result' in quotes_response and 'timeline' in quotes_response['result']:
                timeline_data = quotes_response['result']['timeline']

            if timeline_data and 'instructions' in timeline_data:
                for instruction in timeline_data['instructions']:
                    if 'entries' in instruction:
                        for entry in instruction['entries']:
                            if 'tweet-' in entry.get('entryId', ''):
                                # Extract quote tweet
                                content = entry.get('content', {})
                                if 'itemContent' in content and 'tweet_results' in content['itemContent']:
                                    tweet_data = content['itemContent']['tweet_results'].get('result', {})
                                    if 'tweet' in tweet_data:
                                        tweet_data = tweet_data['tweet']
                                    if 'core' in tweet_data and 'user_results' in tweet_data['core']:
                                        user_data = tweet_data['core']['user_results'].get('result', {})
                                        # CRITICAL FIX: Try multiple paths for screen_name
                                        username = None

                                        # Path 1: core.screen_name
                                        if 'core' in user_data:
                                            username = user_data['core'].get('screen_name')

                                        # Path 2: legacy.screen_name
                                        if not username and 'legacy' in user_data:
                                            username = user_data['legacy'].get('screen_name')

                                        if username:
                                            interactions['quoted_by'].append(username)
    except Exception as e:
        print(f"Error fetching quotes for {tweet_id}: {e}")

    return interactions

def get_community_members(community_id):
    """Get all members and moderators of a community"""
    members_data = {
        'members': [],
        'moderators': []
    }

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
            if 'result' in members_response and 'members_slice' in members_response['result']:
                items_results = members_response['result']['members_slice'].get('items_results', [])

                for item in items_results:
                    if 'result' in item and 'legacy' in item['result']:
                        user = item['result']
                        legacy = user['legacy']

                        member_info = {
                            'username': legacy.get('screen_name', ''),
                            'name': legacy.get('name', ''),
                            'id': legacy.get('id_str', ''),
                            'followers_count': legacy.get('followers_count', 0),
                            'verified': user.get('verification', {}).get('verified', False),
                            'is_blue_verified': user.get('is_blue_verified', False),
                            'description': legacy.get('description', ''),
                            'profile_image_url': legacy.get('profile_image_url_https', ''),
                            'community_role': user.get('community_role', 'Member'),
                            'protected': user.get('privacy', {}).get('protected', False)
                        }

                        # Separate members and moderators based on role
                        if user.get('community_role') == 'Moderator':
                            members_data['moderators'].append(member_info)
                        else:
                            members_data['members'].append(member_info)

                # Check for next page cursor
                cursor = None
                if 'cursor' in members_response and 'bottom' in members_response['cursor']:
                    cursor_data = members_response['cursor']['bottom']
                    if isinstance(cursor_data, dict) and 'next_cursor' in cursor_data:
                        cursor = cursor_data['next_cursor']
                elif 'result' in members_response and 'members_slice' in members_response['result']:
                    slice_info = members_response['result']['members_slice'].get('slice_info', {})
                    if 'next_cursor' in slice_info:
                        cursor = slice_info['next_cursor']

                if not cursor or not items_results:
                    break

                print(f"Found {len(items_results)} users on page {page}")
            else:
                break

        print(f"Found {len(members_data['members'])} members and {len(members_data['moderators'])} moderators")

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
            if 'result' in moderators_response and 'moderators_slice' in moderators_response['result']:
                items_results = moderators_response['result']['moderators_slice'].get('items_results', [])

                for item in items_results:
                    if 'result' in item and 'legacy' in item['result']:
                        user = item['result']
                        legacy = user['legacy']

                        moderator_info = {
                            'username': legacy.get('screen_name', ''),
                            'name': legacy.get('name', ''),
                            'id': legacy.get('id_str', ''),
                            'followers_count': legacy.get('followers_count', 0),
                            'verified': user.get('verification', {}).get('verified', False),
                            'is_blue_verified': user.get('is_blue_verified', False),
                            'description': legacy.get('description', ''),
                            'profile_image_url': legacy.get('profile_image_url_https', ''),
                            'community_role': user.get('community_role', 'Moderator'),
                            'protected': user.get('privacy', {}).get('protected', False)
                        }

                        # Check if already added from members endpoint
                        existing_mod = next((m for m in members_data['moderators'] if m['id'] == moderator_info['id']), None)
                        if not existing_mod:
                            members_data['moderators'].append(moderator_info)

                # Check for next page cursor for moderators
                mod_cursor = None
                if 'cursor' in moderators_response and 'bottom' in moderators_response['cursor']:
                    cursor_data = moderators_response['cursor']['bottom']
                    if isinstance(cursor_data, dict) and 'next_cursor' in cursor_data:
                        mod_cursor = cursor_data['next_cursor']
                elif 'result' in moderators_response and 'moderators_slice' in moderators_response['result']:
                    slice_info = moderators_response['result']['moderators_slice'].get('slice_info', {})
                    if 'next_cursor' in slice_info:
                        mod_cursor = slice_info['next_cursor']

                if not mod_cursor or not items_results:
                    break

                print(f"Found {len(items_results)} moderators on page {mod_page}")
            else:
                break

    except Exception as e:
        print(f"Error fetching additional moderators for {community_id}: {e}")

    return members_data

def save_members_to_file(members_data, community_id, raw_data_dir):
    """Save community members list for comparison"""
    # Save member data for identification purposes
    member_list = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'members': [
            {
                'username': member.get('username'),
                'display_name': member.get('display_name'),
                'user_id': member.get('user_id')
            }
            for member in members_data.get('members', [])
            if member.get('username')
        ],
        'moderators': [
            {
                'username': mod.get('username'),
                'display_name': mod.get('display_name'),
                'user_id': mod.get('user_id')
            }
            for mod in members_data.get('moderators', [])
            if mod.get('username')
        ]
    }

    filename = os.path.join(raw_data_dir, f"{community_id}_members.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(member_list, f, indent=2, ensure_ascii=False)

    total_members = len(member_list['members'])
    total_moderators = len(member_list['moderators'])
    print(f"Saved {total_members} members and {total_moderators} moderators to: {filename}")
    return filename

def process_tweet_data(tweet_data, entry_id):
    """Process individual tweet data and extract all information"""
    if not tweet_data:
        print(f"DEBUG: No tweet_data for entry {entry_id}")
        return None

    print(f"DEBUG: Tweet data keys: {list(tweet_data.keys()) if isinstance(tweet_data, dict) else 'Not a dict'}")

    if 'legacy' not in tweet_data:
        print(f"DEBUG: No legacy data in tweet_data for entry {entry_id}")
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
            'description': user_data.get('description', ''),
            'followers_count': user_data.get('followers_count', 0),
            'friends_count': user_data.get('friends_count', 0),
            'verified': user_data.get('verified', False),
        },

        # Engagement metrics
        'engagement': {
            'retweet_count': legacy.get('retweet_count', 0),
            'favorite_count': legacy.get('favorite_count', 0),
            'reply_count': legacy.get('reply_count', 0),
            'quote_count': legacy.get('quote_count', 0),
        },

        # Content flags
        'is_retweet': legacy.get('retweeted_status_id_str') is not None,
        'is_reply': legacy.get('in_reply_to_status_id_str') is not None,
        'is_quote': legacy.get('quoted_status_id_str') is not None,

        # References
        'in_reply_to_status_id': legacy.get('in_reply_to_status_id_str'),
        'in_reply_to_user_id': legacy.get('in_reply_to_user_id_str'),
        'retweeted_status_id': legacy.get('retweeted_status_id_str'),
        'quoted_status_id': legacy.get('quoted_status_id_str'),

        # Additional metadata
        'lang': legacy.get('lang', ''),
        'source': legacy.get('source', ''),
        'possibly_sensitive': legacy.get('possibly_sensitive', False),
    }

    # Get user interactions for this tweet
    if tweet_id:
        print(f"Fetching interactions for tweet {tweet_id}")
        interactions = get_users_who_interacted(tweet_id)
        processed_tweet['user_interactions'] = interactions
        print(f"Found {len(interactions['retweeted_by'])} retweets, {len(interactions['liked_by'])} likes, {len(interactions['replied_by'])} replies, {len(interactions['quoted_by'])} quotes")
    else:
        processed_tweet['user_interactions'] = {
            'retweeted_by': [],
            'liked_by': [],
            'replied_by': [],
            'quoted_by': []
        }

    # Extract entities (hashtags, mentions, urls, media)
    entities = legacy.get('entities', {})
    processed_tweet['entities'] = {
        'hashtags': [tag.get('text', '') for tag in entities.get('hashtags', [])],
        'mentions': [{'screen_name': m.get('screen_name', ''), 'name': m.get('name', '')}
                    for m in entities.get('user_mentions', [])],
        'urls': [{'url': u.get('url', ''), 'expanded_url': u.get('expanded_url', ''),
                 'display_url': u.get('display_url', '')} for u in entities.get('urls', [])],
        'media': [{'type': m.get('type', ''), 'url': m.get('media_url_https', ''),
                  'expanded_url': m.get('expanded_url', '')} for m in entities.get('media', [])]
    }

    # Handle retweeted content
    if 'retweeted_status_result' in tweet_data:
        retweeted = tweet_data['retweeted_status_result'].get('result', {})
        if 'legacy' in retweeted:
            processed_tweet['retweeted_status'] = process_tweet_data(retweeted, '')

    # Handle quoted content
    if 'quoted_status_result' in tweet_data:
        quoted = tweet_data['quoted_status_result'].get('result', {})
        if 'legacy' in quoted:
            processed_tweet['quoted_status'] = process_tweet_data(quoted, '')

    return processed_tweet

def get_community_posts(community_id, cursor=None):
    """Fetch posts from a community"""
    endpoint = "/community-tweets"
    params = f"communityId={community_id}&searchType=Default&rankingMode=Recency&count=500"

    if cursor:
        params += f"&cursor={cursor}"

    return make_request(endpoint, params)

def fetch_all_community_posts(community_id, config):
    """Fetch all posts from a community within specified days"""
    all_posts = []
    cursor = None
    page = 0
    days_back = config['data']['days_back']
    post_limit = config['data']['post_limit']
    request_delay = config['rate_limiting']['request_delay']
    raw_data_dir = config['output']['raw_data_dir']

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

        # Process response directly without saving raw data

        # Extract all content using new extraction method
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

        # Rate limiting handled by make_request function

        # Safety limit
        if page >= 500:
            print("Reached page limit")
            break

    return all_posts

def find_non_member_interactions(all_posts, members_data):
    """Find users who interacted with posts but are not community members"""
    # Create set of member usernames for fast lookup
    member_usernames = set()
    for member in members_data['members']:
        if member['username']:
            member_usernames.add(member['username'].lower())

    for moderator in members_data['moderators']:
        if moderator['username']:
            member_usernames.add(moderator['username'].lower())

    non_member_interactions = []

    for community_id, posts in all_posts.items():
        for post in posts:
            if 'user_interactions' in post:
                interactions = post['user_interactions']

                # Check all interaction types
                for interaction_type in ['retweeted_by', 'liked_by', 'replied_by', 'quoted_by']:
                    for username in interactions.get(interaction_type, []):
                        if username.lower() not in member_usernames:
                            non_member_interactions.append({
                                'username': username,
                                'interaction_type': interaction_type,
                                'post_id': post.get('tweet_id', ''),
                                'post_text': post.get('text', '')[:100] + '...' if len(post.get('text', '')) > 100 else post.get('text', ''),
                                'community_id': community_id
                            })

    return non_member_interactions

def save_non_members_to_file(non_member_interactions, community_id, raw_data_dir):
    """Save external users who interacted with community posts"""
    # Group interactions by user for cleaner output
    users_data = {}
    for interaction in non_member_interactions:
        username = interaction['username']
        if username not in users_data:
            users_data[username] = {
                'username': username,
                'interactions': []
            }
        users_data[username]['interactions'].append({
            'type': interaction['interaction_type'],
            'post_id': interaction['post_id'],
            'post_text': interaction['post_text']
        })

    # Create final structure
    external_users = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_external_users': len(users_data),
            'total_interactions': len(non_member_interactions),
            'interaction_types': {}
        },
        'external_users': list(users_data.values())
    }

    # Count by interaction type
    for interaction in non_member_interactions:
        interaction_type = interaction['interaction_type']
        if interaction_type not in external_users['summary']['interaction_types']:
            external_users['summary']['interaction_types'][interaction_type] = 0
        external_users['summary']['interaction_types'][interaction_type] += 1

    filename = os.path.join(raw_data_dir, f"{community_id}_non_members.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(external_users, f, indent=2, ensure_ascii=False)

    print(f"Non-members saved to: {filename}")
    print(f"Found {len(users_data)} non-members with {len(non_member_interactions)} total interactions")
    return filename

def main():
    """Main function - focus only on finding external users who interact with community posts"""
    global rate_limiter, request_count, start_time

    try:
        # Load configuration
        config = load_config()

        # Initialize rate limiter with config values
        requests_per_second = config['rate_limiting']['requests_per_second']
        rate_limiter = RateLimiter(requests_per_second)

        # Reset counters
        request_count = 0
        start_time = None

        print(f"Initialized rate limiter: {requests_per_second} requests/second")
        print(f"Rate limiting: Maximum {requests_per_second} requests per second")

        community_ids = config['communities']['ids']
        raw_data_dir = config['output']['raw_data_dir']
        community_delay = config['rate_limiting']['community_delay']

        total_external_interactions = 0
        all_external_users = set()

        for i, community_id in enumerate(community_ids):
            print(f"\n{'='*50}")
            print(f"Processing community {i+1}/{len(community_ids)}: {community_id}")
            print(f"{'='*50}")

            # Step 1: Get community members for comparison
            print(f"Fetching community members and moderators...")
            members_data = get_community_members(community_id)
            save_members_to_file(members_data, community_id, raw_data_dir)

            # Step 2: Get community posts and analyze interactions
            print(f"Fetching community posts to find external interactions...")
            posts = fetch_all_community_posts(community_id, config)
            print(f"Analyzed {len(posts)} posts for interactions")

            # Step 3: Identify external users (non-members who interacted)
            print(f"Identifying external users who interacted with community posts...")
            external_interactions = find_non_member_interactions({community_id: posts}, members_data)
            save_non_members_to_file(external_interactions, community_id, raw_data_dir)

            # Track totals
            total_external_interactions += len(external_interactions)
            community_external_users = set(interaction['username'] for interaction in external_interactions)
            all_external_users.update(community_external_users)

            # Delay between communities
            if i < len(community_ids) - 1:
                print(f"Waiting {community_delay} seconds before next community...")
                time.sleep(community_delay)

        # Final summary
        print(f"\n{'='*60}")
        print(f"ANALYSIS COMPLETE")
        print(f"{'='*60}")
        print(f"Communities processed: {len(community_ids)}")
        print(f"Total external interactions found: {total_external_interactions}")
        print(f"Unique external users: {len(all_external_users)}")

        # API usage summary
        if start_time:
            total_time = time.time() - start_time
            avg_rate = request_count / total_time if total_time > 0 else 0
            print(f"\nAPI Usage Summary:")
            print(f"- Total requests: {request_count}")
            print(f"- Total time: {total_time:.1f} seconds")
            print(f"- Average rate: {avg_rate:.2f} requests/second")

        print(f"\nOutput files saved in {raw_data_dir}:")
        print(f"- [community_id]_members.json (community members)")
        print(f"- [community_id]_non_members.json (non-members who interacted)")



    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Non-Members Extractor from Member Interactions

This script analyzes member interactions to find non-community users by:
1. Loading raw/[community_id]_members_interactions.json
2. Going through all posts and finding users that are:
   - Mentioned in posts (@username)
   - Original posters of retweeted posts
   - Original posters of quoted posts
   - Original posters of posts being replied to
3. Saving this new list of non-members into raw/[community_id]_non_members.json

This helps identify users who are being interacted with by community members
but are not themselves members of the community.
"""

import json
import os
import re
import toml
from datetime import datetime
from collections import defaultdict

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

def load_member_interactions(community_id, raw_data_dir):
    """Load the member interactions data"""
    filename = os.path.join(raw_data_dir, f"{community_id}_members_interactions.json")

    if not os.path.exists(filename):
        print(f"Error: Member interactions file not found: {filename}")
        print("Please run fetch_member_interactions.py first to generate the interactions data.")
        return None

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading member interactions file {filename}: {e}")
        return None

def extract_mentions_from_text(text):
    """Extract @mentions from post text"""
    if not text:
        return []

    # Find all @mentions in the text
    mentions = re.findall(r'@(\w+)', text)
    # Convert to lowercase for consistency
    return [mention.lower() for mention in mentions]

def get_community_member_ids(interactions_data):
    """Get set of community member user IDs and usernames for filtering"""
    member_ids = set()
    member_usernames = set()

    for member in interactions_data.get('members_interactions', []):
        if member.get('user_id'):
            member_ids.add(member['user_id'])
        if member.get('username'):
            member_usernames.add(member['username'].lower())

    return member_ids, member_usernames

def analyze_member_interactions(interactions_data):
    """Analyze all member interactions to find non-member users"""

    # Get community member IDs to filter them out
    member_ids, member_usernames = get_community_member_ids(interactions_data)

    # Dictionary to store non-member users with their interaction counts
    non_members = defaultdict(lambda: {
        'user_id': None,
        'username': None,
        'display_name': None,
        'interactions': {
            'mentioned_by': [],
            'retweeted_by': [],
            'quoted_by': [],
            'replied_to_by': []
        },
        'interaction_counts': {
            'mentions': 0,
            'retweets': 0,
            'quotes': 0,
            'replies': 0,
            'total': 0
        }
    })

    print(f"Analyzing interactions from {len(interactions_data.get('members_interactions', []))} community members...")

    for member in interactions_data.get('members_interactions', []):
        member_username = member.get('username', '')
        member_user_id = member.get('user_id', '')

        print(f"  Processing interactions from @{member_username}")

        # Analyze posts
        for post in member.get('posts', []):
            post_id = post.get('post_id', '')
            post_text = post.get('text', '')
            created_at = post.get('created_at', '')

            # 1. Find mentions in post text
            mentions = extract_mentions_from_text(post_text)
            for mentioned_user in mentions:
                if mentioned_user not in member_usernames and mentioned_user != member_username.lower():
                    non_members[mentioned_user]['username'] = mentioned_user
                    non_members[mentioned_user]['interactions']['mentioned_by'].append({
                        'by_member': member_username,
                        'by_member_id': member_user_id,
                        'post_id': post_id,
                        'created_at': created_at
                    })
                    non_members[mentioned_user]['interaction_counts']['mentions'] += 1

            # 2. Find original poster of retweeted post
            if post.get('is_retweet') and post.get('original_post_creator_id'):
                rt_creator_id = post['original_post_creator_id']
                rt_creator_username = post.get('original_post_creator_username', '')

                if rt_creator_id not in member_ids and rt_creator_username.lower() not in member_usernames:
                    key = rt_creator_username.lower() if rt_creator_username else rt_creator_id
                    non_members[key]['user_id'] = rt_creator_id
                    non_members[key]['username'] = rt_creator_username
                    non_members[key]['interactions']['retweeted_by'].append({
                        'by_member': member_username,
                        'by_member_id': member_user_id,
                        'post_id': post_id,
                        'retweeted_post_id': post.get('retweeted_post_id'),
                        'created_at': created_at
                    })
                    non_members[key]['interaction_counts']['retweets'] += 1

            # 3. Find original poster of quoted post
            if post.get('is_quote') and post.get('original_post_creator_id'):
                qt_creator_id = post['original_post_creator_id']
                qt_creator_username = post.get('original_post_creator_username', '')

                if qt_creator_id not in member_ids and qt_creator_username.lower() not in member_usernames:
                    key = qt_creator_username.lower() if qt_creator_username else qt_creator_id
                    non_members[key]['user_id'] = qt_creator_id
                    non_members[key]['username'] = qt_creator_username
                    non_members[key]['interactions']['quoted_by'].append({
                        'by_member': member_username,
                        'by_member_id': member_user_id,
                        'post_id': post_id,
                        'quoted_post_id': post.get('quoted_post_id'),
                        'created_at': created_at
                    })
                    non_members[key]['interaction_counts']['quotes'] += 1

        # Analyze replies
        for reply in member.get('replies', []):
            reply_id = reply.get('post_id', '')
            reply_text = reply.get('text', '')
            created_at = reply.get('created_at', '')

            # 1. Find mentions in reply text
            mentions = extract_mentions_from_text(reply_text)
            for mentioned_user in mentions:
                if mentioned_user not in member_usernames and mentioned_user != member_username.lower():
                    non_members[mentioned_user]['username'] = mentioned_user
                    non_members[mentioned_user]['interactions']['mentioned_by'].append({
                        'by_member': member_username,
                        'by_member_id': member_user_id,
                        'post_id': reply_id,
                        'created_at': created_at,
                        'is_reply': True
                    })
                    non_members[mentioned_user]['interaction_counts']['mentions'] += 1

            # 2. Find original poster being replied to
            if reply.get('reply_to_user_id'):
                replied_to_id = reply['reply_to_user_id']
                replied_to_username = reply.get('reply_to_username', '')

                if replied_to_id not in member_ids and replied_to_username.lower() not in member_usernames:
                    key = replied_to_username.lower() if replied_to_username else replied_to_id
                    non_members[key]['user_id'] = replied_to_id
                    non_members[key]['username'] = replied_to_username
                    non_members[key]['interactions']['replied_to_by'].append({
                        'by_member': member_username,
                        'by_member_id': member_user_id,
                        'reply_id': reply_id,
                        'original_post_id': reply.get('reply_to_post_id'),
                        'created_at': created_at
                    })
                    non_members[key]['interaction_counts']['replies'] += 1

    # Calculate total interaction counts and clean up data
    final_non_members = []
    for key, data in non_members.items():
        data['interaction_counts']['total'] = (
            data['interaction_counts']['mentions'] +
            data['interaction_counts']['retweets'] +
            data['interaction_counts']['quotes'] +
            data['interaction_counts']['replies']
        )

        # Only include users we have some information about
        if data['username'] or data['user_id']:
            final_non_members.append(data)

    # Sort by total interaction count (descending)
    final_non_members.sort(key=lambda x: x['interaction_counts']['total'], reverse=True)

    return final_non_members

def save_non_members_data(community_id, non_members, raw_data_dir):
    """Save non-members data to file"""

    # Create summary statistics
    total_interactions = sum(nm['interaction_counts']['total'] for nm in non_members)
    mentions_total = sum(nm['interaction_counts']['mentions'] for nm in non_members)
    retweets_total = sum(nm['interaction_counts']['retweets'] for nm in non_members)
    quotes_total = sum(nm['interaction_counts']['quotes'] for nm in non_members)
    replies_total = sum(nm['interaction_counts']['replies'] for nm in non_members)

    # Create output data structure
    output_data = {
        'community_id': community_id,
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_non_members_found': len(non_members),
            'total_interactions_with_non_members': total_interactions,
            'interaction_breakdown': {
                'mentions': mentions_total,
                'retweets': retweets_total,
                'quotes': quotes_total,
                'replies': replies_total
            }
        },
        'non_members': non_members
    }

    # Save to file
    filename = os.path.join(raw_data_dir, f"{community_id}_non_members.json")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\nNon-members data saved to: {filename}")
    print(f"Summary:")
    print(f"- Non-members found: {len(non_members)}")
    print(f"- Total interactions: {total_interactions}")
    print(f"  * Mentions: {mentions_total}")
    print(f"  * Retweets: {retweets_total}")
    print(f"  * Quotes: {quotes_total}")
    print(f"  * Replies: {replies_total}")

    # Show top 10 most interacted with non-members
    if non_members:
        print(f"\nTop 10 most interacted with non-members:")
        for i, nm in enumerate(non_members[:10], 1):
            username = nm.get('username') or 'Unknown'
            total = nm['interaction_counts']['total']
            print(f"  {i:2d}. @{username} ({total} interactions)")

    return filename, output_data['summary']

def main():
    """Main function - extract non-members from member interactions"""

    try:
        # Load configuration
        config = load_config()
        if not config:
            return

        print(f"Non-Members Extractor")
        print(f"=" * 50)

        community_ids = config['communities']['ids']
        raw_data_dir = config['output']['raw_data_dir']

        for i, community_id in enumerate(community_ids):
            print(f"\n{'='*50}")
            print(f"Processing community {i+1}/{len(community_ids)}: {community_id}")
            print(f"{'='*50}")

            # Load member interactions data
            interactions_data = load_member_interactions(community_id, raw_data_dir)
            if not interactions_data:
                print(f"Skipping community {community_id} - no interactions data found")
                continue

            # Analyze interactions to find non-members
            print(f"\nExtracting non-member users from interactions...")
            non_members = analyze_member_interactions(interactions_data)

            # Save non-members data
            if non_members:
                save_non_members_data(community_id, non_members, raw_data_dir)
            else:
                print(f"No non-member interactions found for community {community_id}")

        print(f"\n{'='*50}")
        print(f"NON-MEMBERS EXTRACTION COMPLETE")
        print(f"{'='*50}")
        print(f"Communities processed: {len(community_ids)}")
        print(f"\nOutput files saved in {raw_data_dir}:")
        print(f"- [community_id]_non_members.json")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

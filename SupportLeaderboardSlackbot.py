import os
import sys
import time
from datetime import datetime, timedelta
from collections import Counter
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Configuration
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "xoxb-your-token-here")
SUPPORT_CHANNEL_ID = os.environ.get("SUPPORT_CHANNEL_ID", "C01ABC2DEF3")  # #product-support-quick-questions
LEADERBOARD_CHANNEL_ID = os.environ.get("LEADERBOARD_CHANNEL_ID", "C01ABC2DEF3")  # Where to post leaderboard
RESOLUTION_EMOJI = "white_check_mark"  # The ✅ emoji (without colons)

# Rate limiting configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1  # seconds

client = WebClient(token=SLACK_BOT_TOKEN)

def retry_api_call(api_func, max_retries=MAX_RETRIES):
    """
    Wrapper for Slack API calls with exponential backoff retry logic.
    Handles rate limiting and transient errors.
    """
    retry_delay = INITIAL_RETRY_DELAY
    
    for attempt in range(max_retries):
        try:
            return api_func()
            
        except SlackApiError as e:
            error_code = e.response.get("error", "")
            
            # Handle rate limiting
            if error_code == "rate_limited":
                retry_after = int(e.response.headers.get("Retry-After", retry_delay))
                print(f"Rate limited. Waiting {retry_after} seconds before retry {attempt + 1}/{max_retries}...")
                time.sleep(retry_after)
                retry_delay = retry_after
                
            # Handle other retryable errors
            elif error_code in ["service_unavailable", "internal_error"]:
                if attempt < max_retries - 1:
                    print(f"Transient error ({error_code}). Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    raise
                    
            # Non-retryable errors
            else:
                raise
    
    raise Exception(f"Max retries ({max_retries}) exceeded")

def get_week_range(weeks_ago=0):
    """
    Get the Monday-Sunday range for a specific week.
    
    Args:
        weeks_ago: 0 for last week, 1 for two weeks ago, etc.
    """
    today = datetime.now()
    # Find last Monday
    days_since_monday = (today.weekday() + 7) % 7
    if days_since_monday == 0:  # If today is Monday
        days_since_monday = 7
    last_monday = today - timedelta(days=days_since_monday + 7 + (weeks_ago * 7))
    last_monday = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Find corresponding Sunday
    last_sunday = last_monday + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
    return last_monday, last_sunday

def get_channel_messages(channel_id, start_date, end_date):
    """
    Fetch messages from a channel within a date range.
    Uses conversations.history which works with basic channel access.
    """
    # Convert to Unix timestamps
    oldest = start_date.timestamp()
    latest = end_date.timestamp()
    
    all_messages = []
    cursor = None
    
    try:
        while True:
            kwargs = {
                "channel": channel_id,
                "oldest": str(oldest),
                "latest": str(latest),
                "limit": 200  # Slack's max per request
            }
            
            if cursor:
                kwargs["cursor"] = cursor
            
            result = retry_api_call(lambda: client.conversations_history(**kwargs))
            
            messages = result.get("messages", [])
            all_messages.extend(messages)
            
            # Check if there are more messages
            cursor = result.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
            
            # Small delay to avoid rate limiting on pagination
            time.sleep(0.5)
        
        return all_messages
        
    except SlackApiError as e:
        print(f"Error fetching messages: {e.response.get('error', 'Unknown error')}")
        raise

def get_reactions_for_message(channel_id, timestamp):
    """
    Get all reactions for a specific message.
    Returns a dict of {emoji_name: [list of user_ids]}
    """
    try:
        result = retry_api_call(
            lambda: client.reactions_get(
                channel=channel_id,
                timestamp=timestamp,
                full=True  # Get full user info
            )
        )
        
        message = result.get("message", {})
        reactions = message.get("reactions", [])
        
        reaction_data = {}
        for reaction in reactions:
            emoji_name = reaction.get("name")
            users = reaction.get("users", [])
            reaction_data[emoji_name] = users
        
        return reaction_data
        
    except SlackApiError as e:
        # Message might not have reactions or might be deleted
        if e.response.get("error") == "message_not_found":
            return {}
        print(f"Warning: Could not get reactions for message: {e.response.get('error', 'Unknown error')}")
        return {}

def count_resolutions_by_reactions(channel_id, start_date, end_date):
    """
    Count resolutions by finding all messages with the resolution emoji reaction
    within the date range, then counting who added those reactions.
    """
    print(f"\nFetching messages from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
    
    # Get all messages in the date range
    messages = get_channel_messages(channel_id, start_date, end_date)
    print(f"Found {len(messages)} messages in date range")
    
    # Track resolutions per user
    resolutions = Counter()
    resolved_threads = 0
    messages_checked = 0
    
    print("\nChecking messages for resolution reactions...")
    
    for i, message in enumerate(messages):
        messages_checked += 1
        
        # Progress indicator for large channels
        if (i + 1) % 50 == 0:
            print(f"  Checked {i + 1}/{len(messages)} messages...")
        
        # Check if message has any reactions
        if not message.get("reactions"):
            continue
        
        # Check if the resolution emoji is present
        has_resolution_emoji = any(
            r.get("name") == RESOLUTION_EMOJI 
            for r in message.get("reactions", [])
        )
        
        if not has_resolution_emoji:
            continue
        
        resolved_threads += 1
        
        # Get detailed reaction info to see who added the resolution emoji
        timestamp = message.get("ts")
        reactions = get_reactions_for_message(channel_id, timestamp)
        
        # Get users who added the resolution emoji
        resolver_ids = reactions.get(RESOLUTION_EMOJI, [])
        
        # Count resolution for each user who added the emoji
        for user_id in resolver_ids:
            try:
                user_info = retry_api_call(
                    lambda: client.users_info(user=user_id)
                )
                user_name = user_info["user"]["real_name"]
                
                # Skip bot users
                if user_info["user"].get("is_bot"):
                    continue
                
                resolutions[user_name] += 1
                
            except Exception as e:
                print(f"Warning: Could not fetch user info for {user_id}: {e}")
                resolutions[f"User {user_id}"] += 1
        
        # Small delay to avoid rate limiting
        if resolved_threads % 10 == 0:
            time.sleep(0.3)
    
    print(f"\n✓ Processed {messages_checked} messages")
    print(f"✓ Found {resolved_threads} resolved threads")
    print(f"✓ Counted {sum(resolutions.values())} total resolution reactions")
    
    return resolutions

def check_for_duplicate_leaderboard(channel_id, start_date, end_date):
    """
    Check if a leaderboard has already been posted for this week.
    Returns True if duplicate detected.
    """
    try:
        date_range_str = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
        
        # Get bot's own user ID to filter its messages
        auth_result = retry_api_call(lambda: client.auth_test())
        bot_user_id = auth_result.get("user_id")
        
        # Fetch recent messages from leaderboard channel
        result = retry_api_call(
            lambda: client.conversations_history(
                channel=channel_id,
                limit=100
            )
        )
        
        for message in result.get("messages", []):
            # Check if message is from this bot
            if message.get("user") != bot_user_id:
                continue
            
            # Check if it's a leaderboard with this date range
            message_text = message.get("text", "")
            if "Weekly Resolution Leaderboard" in message_text and date_range_str in message_text:
                return True
        
        return False
        
    except Exception as e:
        print(f"Warning: Error checking for duplicate leaderboard: {e}")
        return False

def post_leaderboard(resolutions, start_date, end_date, channel_id):
    """Post the leaderboard to the specified channel"""
    
    # Format the date range
    date_range = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
    
    # Build leaderboard message
    message_blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🏆 Weekly Resolution Leaderboard"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Week of {date_range}*"
            }
        },
        {
            "type": "divider"
        }
    ]
    
    if resolutions:
        # Sort by resolution count (descending)
        sorted_resolutions = resolutions.most_common()
        total_resolutions = sum(resolutions.values())
        
        # Add each agent's stats
        leaderboard_text = ""
        medals = ["🥇", "🥈", "🥉"]
        
        for i, (agent, count) in enumerate(sorted_resolutions):
            medal = medals[i] if i < 3 else "   "
            leaderboard_text += f"{medal} *{agent}*: {count} resolutions\n"
        
        message_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": leaderboard_text
            }
        })
        
        message_blocks.extend([
            {
                "type": "divider"
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"📊 *Total resolutions:* {total_resolutions}"
                    }
                ]
            }
        ])
    else:
        message_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "_No resolutions found this week._"
            }
        })
    
    try:
        retry_api_call(
            lambda: client.chat_postMessage(
                channel=channel_id,
                blocks=message_blocks,
                text=f"Weekly Resolution Leaderboard ({date_range})"
            )
        )
        print(f"\n✓ Leaderboard posted successfully for {date_range}!")
        
    except Exception as e:
        print(f"\n✗ Error posting leaderboard: {e}")
        raise

def main():
    """Main function to generate and post the leaderboard"""
    print("=" * 70)
    print("Support Resolution Leaderboard Generator (Reaction-Based)")
    print("=" * 70)
    
    # Validate configuration
    if SLACK_BOT_TOKEN == "xoxb-your-token-here":
        print("\nError: Please set SLACK_BOT_TOKEN environment variable")
        print("Example: export SLACK_BOT_TOKEN='xoxb-...'")
        sys.exit(1)
    
    if SUPPORT_CHANNEL_ID == "C01ABC2DEF3":
        print("\nError: Please set SUPPORT_CHANNEL_ID environment variable")
        print("Example: export SUPPORT_CHANNEL_ID='C01ABC2DEF3'")
        sys.exit(1)
    
    # Parse command line arguments
    weeks_to_process = [0]  # Default: last week only
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help":
            print("\nUsage:")
            print("  python support_leaderboard.py              # Generate last week's leaderboard")
            print("  python support_leaderboard.py --weeks N    # Generate leaderboards for last N weeks")
            print("  python support_leaderboard.py --week N     # Generate leaderboard for N weeks ago")
            print("\nEnvironment Variables:")
            print("  SLACK_BOT_TOKEN         - Your Slack bot token (required)")
            print("  SUPPORT_CHANNEL_ID      - Channel to read reactions from (required)")
            print("  LEADERBOARD_CHANNEL_ID  - Where to post leaderboard (defaults to SUPPORT_CHANNEL_ID)")
            print("\nExamples:")
            print("  python support_leaderboard.py --weeks 3    # Catch up on last 3 weeks")
            print("  python support_leaderboard.py --week 2     # Generate for 2 weeks ago only")
            sys.exit(0)
        
        elif sys.argv[1] == "--weeks" and len(sys.argv) > 2:
            num_weeks = int(sys.argv[2])
            weeks_to_process = list(range(num_weeks))
            print(f"\nProcessing last {num_weeks} weeks (catching up)")
        
        elif sys.argv[1] == "--week" and len(sys.argv) > 2:
            specific_week = int(sys.argv[2])
            weeks_to_process = [specific_week]
            print(f"\nProcessing week from {specific_week} weeks ago")
    
    # Set leaderboard channel (defaults to support channel if not specified)
    leaderboard_channel = LEADERBOARD_CHANNEL_ID if LEADERBOARD_CHANNEL_ID != "C01ABC2DEF3" else SUPPORT_CHANNEL_ID
    
    print(f"\nConfiguration:")
    print(f"  Support channel: {SUPPORT_CHANNEL_ID}")
    print(f"  Leaderboard channel: {leaderboard_channel}")
    print(f"  Resolution emoji: :{RESOLUTION_EMOJI}:")
    
    # Verify bot connection
    try:
        auth_result = retry_api_call(lambda: client.auth_test())
        print(f"  Bot connected as: {auth_result['user']}")
    except Exception as e:
        print(f"\n✗ Error: Could not authenticate with Slack: {e}")
        sys.exit(1)
    
    # Process each week
    for weeks_ago in weeks_to_process:
        print(f"\n{'─' * 70}")
        
        # Get week date range
        start_date, end_date = get_week_range(weeks_ago)
        date_range = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
        print(f"\nProcessing week: {date_range}")
        
        # Check for duplicate leaderboard
        if check_for_duplicate_leaderboard(leaderboard_channel, start_date, end_date):
            print(f"⚠ Leaderboard already exists for {date_range}. Skipping.")
            continue
        
        # Count resolutions by reading reactions
        resolutions = count_resolutions_by_reactions(SUPPORT_CHANNEL_ID, start_date, end_date)
        
        if resolutions:
            print(f"\nLeaderboard preview:")
            for agent, count in resolutions.most_common(5):
                print(f"  {agent}: {count} resolutions")
            if len(resolutions) > 5:
                print(f"  ... and {len(resolutions) - 5} more agents")
        
        # Post leaderboard
        print(f"\nPosting leaderboard to channel...")
        post_leaderboard(resolutions, start_date, end_date, leaderboard_channel)
        
        # Delay between weeks if processing multiple
        if weeks_ago != weeks_to_process[-1]:
            time.sleep(2)
    
    print(f"\n{'=' * 70}")
    print("✓ Leaderboard generation complete!")
    print("=" * 70)

if __name__ == "__main__":
    main()

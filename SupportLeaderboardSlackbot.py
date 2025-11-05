import os
import sys
import time
import re
from datetime import datetime, timedelta
from collections import Counter
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Configuration
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "xoxb-your-token-here")  # Prefer environment variable
TRACKING_CHANNEL_ID = os.environ.get("TRACKING_CHANNEL_ID", "C01ABC2DEF3")  # Prefer environment variable
WORKFLOW_BOT_ID = None  # Will be auto-detected from tracking channel messages

# Rate limiting configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1  # seconds

# Expected workflow message format validation
WORKFLOW_MESSAGE_PATTERN = re.compile(r"✅.*Thread resolved by <@[A-Z0-9]+>", re.IGNORECASE)

client = WebClient(token=SLACK_BOT_TOKEN)

def detect_workflow_bot_id():
    """
    Auto-detect the bot ID of the workflow by examining recent messages in tracking channel.
    This ensures we only count messages from the official workflow.
    """
    global WORKFLOW_BOT_ID
    
    if WORKFLOW_BOT_ID:
        return WORKFLOW_BOT_ID
    
    try:
        # Fetch recent messages to find the workflow bot
        result = retry_api_call(
            lambda: client.conversations_history(
                channel=TRACKING_CHANNEL_ID,
                limit=100
            )
        )
        
        # Look for messages with the workflow pattern from a bot
        for message in result.get("messages", []):
            if "bot_id" in message and WORKFLOW_MESSAGE_PATTERN.search(message.get("text", "")):
                WORKFLOW_BOT_ID = message["bot_id"]
                print(f"Detected workflow bot ID: {WORKFLOW_BOT_ID}")
                return WORKFLOW_BOT_ID
        
        print("Warning: Could not auto-detect workflow bot ID. Will count all matching messages.")
        return None
        
    except Exception as e:
        print(f"Warning: Error detecting workflow bot ID: {e}")
        return None

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

def is_valid_workflow_message(message):
    """
    Validate that a message came from the official workflow.
    Checks bot_id and message format.
    """
    # Must be from a bot
    if "bot_id" not in message:
        return False
    
    # If we detected the workflow bot ID, verify it matches
    if WORKFLOW_BOT_ID and message["bot_id"] != WORKFLOW_BOT_ID:
        return False
    
    # Check message format matches workflow pattern
    message_text = message.get("text", "")
    if not WORKFLOW_MESSAGE_PATTERN.search(message_text):
        return False
    
    return True

def extract_resolver_from_message(message):
    """
    Extract the user who resolved the thread from the workflow message.
    Returns user ID extracted from the message text.
    """
    message_text = message.get("text", "")
    
    # Extract user mention from "Thread resolved by <@U12345>"
    match = re.search(r"<@([A-Z0-9]+)>", message_text)
    if match:
        return match.group(1)
    
    return None

def check_for_duplicate_leaderboard(start_date, end_date):
    """
    Check if a leaderboard has already been posted for this week.
    Returns True if duplicate detected.
    """
    try:
        # Search for existing leaderboards in the channel
        date_range_str = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
        
        # Fetch recent messages to check for duplicates
        result = retry_api_call(
            lambda: client.conversations_history(
                channel=TRACKING_CHANNEL_ID,
                limit=100  # Check last 100 messages
            )
        )
        
        for message in result.get("messages", []):
            # Check if message is from this bot and contains the same date range
            if message.get("bot_id") and "Weekly Resolution Leaderboard" in message.get("text", ""):
                if date_range_str in message.get("text", ""):
                    return True
        
        return False
        
    except Exception as e:
        print(f"Warning: Error checking for duplicate leaderboard: {e}")
        return False  # Proceed with caution if check fails

def get_resolutions_for_week(start_date, end_date):
    """
    Fetch all valid resolution messages from tracking channel for the given week.
    Validates messages and deduplicates by thread.
    """
    resolutions = Counter()
    seen_threads = set()  # Track unique threads to prevent double-counting
    
    # Convert to Unix timestamps
    oldest = start_date.timestamp()
    latest = end_date.timestamp()
    
    try:
        # Fetch messages from the channel with pagination support
        cursor = None
        all_messages = []
        
        while True:
            result = retry_api_call(
                lambda: client.conversations_history(
                    channel=TRACKING_CHANNEL_ID,
                    oldest=str(oldest),
                    latest=str(latest),
                    limit=200,  # Slack's max per request
                    cursor=cursor
                )
            )
            
            all_messages.extend(result.get("messages", []))
            
            # Check if there are more messages
            cursor = result.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
            
            # Small delay to avoid rate limiting on pagination
            time.sleep(0.5)
        
        print(f"Fetched {len(all_messages)} total messages from tracking channel")
        
        # Process and validate messages
        valid_count = 0
        invalid_count = 0
        duplicate_count = 0
        
        for message in all_messages:
            # Validate message is from workflow
            if not is_valid_workflow_message(message):
                invalid_count += 1
                continue
            
            valid_count += 1
            
            # Extract thread identifier to detect duplicates
            message_text = message.get("text", "")
            thread_match = re.search(r"Thread: (https://[^\s]+)", message_text)
            
            if thread_match:
                thread_url = thread_match.group(1)
                
                # Skip if we've already counted this thread
                if thread_url in seen_threads:
                    duplicate_count += 1
                    continue
                
                seen_threads.add(thread_url)
            
            # Extract the resolver user ID
            user_id = extract_resolver_from_message(message)
            
            if user_id:
                # Get user's real name with retry logic
                try:
                    user_info = retry_api_call(
                        lambda: client.users_info(user=user_id)
                    )
                    user_name = user_info["user"]["real_name"]
                    resolutions[user_name] += 1
                    
                except Exception as e:
                    print(f"Warning: Could not fetch user info for {user_id}: {e}")
                    # Use user ID as fallback
                    resolutions[f"User {user_id}"] += 1
        
        print(f"Processed {valid_count} valid workflow messages, {invalid_count} invalid, {duplicate_count} duplicates")
        
        return resolutions
        
    except Exception as e:
        print(f"Error fetching messages: {e}")
        return resolutions

def post_leaderboard(resolutions, start_date, end_date):
    """Post the leaderboard to the tracking channel"""
    
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
                "text": "_No resolutions logged this week._"
            }
        })
    
    try:
        retry_api_call(
            lambda: client.chat_postMessage(
                channel=TRACKING_CHANNEL_ID,
                blocks=message_blocks,
                text=f"Weekly Resolution Leaderboard ({date_range})"
            )
        )
        print(f"✓ Leaderboard posted successfully for {date_range}!")
        
    except Exception as e:
        print(f"✗ Error posting leaderboard: {e}")
        raise

def main():
    """Main function to generate and post the leaderboard"""
    print("=" * 60)
    print("Support Resolution Leaderboard Generator")
    print("=" * 60)
    
    # Validate configuration
    if SLACK_BOT_TOKEN == "xoxb-your-token-here":
        print("Error: Please set SLACK_BOT_TOKEN environment variable or update script")
        sys.exit(1)
    
    if TRACKING_CHANNEL_ID == "C01ABC2DEF3":
        print("Error: Please set TRACKING_CHANNEL_ID environment variable or update script")
        sys.exit(1)
    
    # Parse command line arguments for week specification
    weeks_to_process = [0]  # Default: last week only
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help":
            print("\nUsage:")
            print("  python support_leaderboard.py              # Generate last week's leaderboard")
            print("  python support_leaderboard.py --weeks N    # Generate leaderboards for last N weeks")
            print("  python support_leaderboard.py --week N     # Generate leaderboard for N weeks ago")
            print("\nExamples:")
            print("  python support_leaderboard.py --weeks 3    # Catch up on last 3 weeks")
            print("  python support_leaderboard.py --week 2     # Generate for 2 weeks ago only")
            sys.exit(0)
        
        elif sys.argv[1] == "--weeks" and len(sys.argv) > 2:
            num_weeks = int(sys.argv[2])
            weeks_to_process = list(range(num_weeks))
            print(f"Processing last {num_weeks} weeks (catching up)")
        
        elif sys.argv[1] == "--week" and len(sys.argv) > 2:
            specific_week = int(sys.argv[2])
            weeks_to_process = [specific_week]
            print(f"Processing week from {specific_week} weeks ago")
    
    # Auto-detect workflow bot ID
    print("\nDetecting workflow bot ID...")
    detect_workflow_bot_id()
    
    # Process each week
    for weeks_ago in weeks_to_process:
        print(f"\n{'─' * 60}")
        
        # Get week date range
        start_date, end_date = get_week_range(weeks_ago)
        date_range = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
        print(f"Processing week: {date_range}")
        
        # Check for duplicate leaderboard
        if check_for_duplicate_leaderboard(start_date, end_date):
            print(f"⚠ Leaderboard already exists for {date_range}. Skipping to prevent duplicate.")
            continue
        
        # Get resolution counts
        print("Fetching and validating resolution messages...")
        resolutions = get_resolutions_for_week(start_date, end_date)
        
        if resolutions:
            print(f"Found {sum(resolutions.values())} resolutions from {len(resolutions)} agents")
        else:
            print("No resolutions found for this week")
        
        # Post leaderboard
        print("Posting leaderboard...")
        post_leaderboard(resolutions, start_date, end_date)
        
        # Small delay between weeks if processing multiple
        if weeks_ago != weeks_to_process[-1]:
            time.sleep(2)
    
    print(f"\n{'=' * 60}")
    print("✓ Leaderboard generation complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()

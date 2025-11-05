import os
from datetime import datetime, timedelta
from collections import Counter
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Configuration
SLACK_BOT_TOKEN = "xoxb-your-token-here"  # Replace with your bot token
TRACKING_CHANNEL_ID = "C01ABC2DEF3"  # Replace with your channel ID

client = WebClient(token=SLACK_BOT_TOKEN)

def get_last_week_range():
    """Get the Monday-Sunday range for the previous week"""
    today = datetime.now()
    # Find last Monday
    days_since_monday = (today.weekday() + 7) % 7
    if days_since_monday == 0:  # If today is Monday
        days_since_monday = 7
    last_monday = today - timedelta(days=days_since_monday + 7)
    last_monday = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Find last Sunday
    last_sunday = last_monday + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
    return last_monday, last_sunday

def get_resolutions_for_week(start_date, end_date):
    """Fetch all resolution messages from tracking channel for the given week"""
    resolutions = Counter()
    
    # Convert to Unix timestamps
    oldest = start_date.timestamp()
    latest = end_date.timestamp()
    
    try:
        # Fetch messages from the channel
        result = client.conversations_history(
            channel=TRACKING_CHANNEL_ID,
            oldest=str(oldest),
            latest=str(latest),
            limit=1000
        )
        
        messages = result["messages"]
        
        # Count messages per user
        for message in messages:
            if "user" in message and "bot_id" not in message:
                # Get user's real name
                user_info = client.users_info(user=message["user"])
                user_name = user_info["user"]["real_name"]
                resolutions[user_name] += 1
        
        return resolutions
        
    except SlackApiError as e:
        print(f"Error fetching messages: {e.response['error']}")
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
        client.chat_postMessage(
            channel=TRACKING_CHANNEL_ID,
            blocks=message_blocks,
            text=f"Weekly Resolution Leaderboard ({date_range})"
        )
        print(f"Leaderboard posted successfully!")
        
    except SlackApiError as e:
        print(f"Error posting message: {e.response['error']}")

def main():
    """Main function to generate and post the leaderboard"""
    print("Generating weekly leaderboard...")
    
    # Get last week's date range
    start_date, end_date = get_last_week_range()
    print(f"Fetching resolutions from {start_date} to {end_date}")
    
    # Get resolution counts
    resolutions = get_resolutions_for_week(start_date, end_date)
    
    # Post leaderboard
    post_leaderboard(resolutions, start_date, end_date)

if __name__ == "__main__":
    main()
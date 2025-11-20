# Support Leaderboard Slackbot

A Slack bot that tracks support resolutions and posts weekly leaderboards based on reaction emojis.

## How It Works

The bot scans a support channel for messages with resolution emojis (`:white_check_mark:` or `:check:`) and counts who added those reactions. It then posts a ranked leaderboard to celebrate top contributors.

## Setup

### Environment Variables

```bash
export SLACK_BOT_TOKEN='xoxb-your-token-here'
export SUPPORT_CHANNEL_ID='C01ABC2DEF3'        # Channel to monitor
export LEADERBOARD_CHANNEL_ID='C01ABC2DEF3'    # Where to post (optional)
```

### Required Slack Bot Scopes

- `channels:history` - Read messages
- `reactions:read` - Read emoji reactions
- `chat:write` - Post leaderboard
- `users:read` - Get user names

### Install Dependencies

```bash
pip install slack-sdk
```

## Usage

```bash
# Generate last week's leaderboard
python SupportLeaderboardSlackbot.py

# Generate leaderboards for last N weeks
python SupportLeaderboardSlackbot.py --weeks 3

# Generate leaderboard for a specific week (N weeks ago)
python SupportLeaderboardSlackbot.py --week 2

# Show help
python SupportLeaderboardSlackbot.py --help
```

## Features

- Automatic rate limiting with exponential backoff
- Duplicate leaderboard detection
- Support for multiple resolution emojis
- Medal rankings for top 3 contributors

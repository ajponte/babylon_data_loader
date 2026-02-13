#!/bin/bash

# A script to monitor the status of GitHub Actions CI builds for the current repository.
# It uses the GitHub CLI ('gh') to fetch the latest run statuses.

# Usage:
# 1. Make the script executable: chmod +x monitor-ci.sh
# 2. Run it: ./monitor-ci.sh [branch_name]
#
# If no branch_name is provided, it will show runs for your current git branch.

# --- Script ---

# Check if gh is installed
if ! command -v gh &> /dev/null
then
    echo "GitHub CLI 'gh' could not be found. Please install it to use this script." >&2
    echo "Installation instructions: https://cli.github.com/" >&2
    exit 1
fi

# Get branch from argument or use current branch
BRANCH=${1:-$(git rev-parse --abbrev-ref HEAD)}
REFRESH_RATE=10 # seconds

if [ -z "$BRANCH" ]; then
    echo "Could not determine git branch. Please specify one as an argument." >&2
    exit 1
fi

# Loop to refresh the status
while true; do
  clear
  echo "Monitoring CI runs for branch: '$BRANCH'"
  echo "Refreshes every $REFRESH_RATE seconds. Press [CTRL+C] to stop."
  echo "Last updated: $(date)"
  echo "---"
  gh run list --branch "$BRANCH" --limit 10
  sleep $REFRESH_RATE
done

#!/bin/bash

# Check if git diff only contains uv.lock files with at most 1 line updated per file

# Get list of changed files
changed_files=$(git diff --name-only)

# Check if there are any changes
if [ -z "$changed_files" ]; then
    echo "✓ No changes detected"
    exit 0
fi

# Check if all changed files are uv.lock files
for file in $changed_files; do
    if [[ ! "$file" =~ uv\.lock$ ]]; then
        echo "✗ Non uv.lock file changed: $file"
        exit 1
    fi
done

# Check each uv.lock file has at most 1 line updated
all_good=true
for file in $changed_files; do
    # Count changed lines (additions + deletions)
    stats=$(git diff --numstat "$file" | awk '{print $1 + $2}')
    
    if [ "$stats" -gt 1 ]; then
        echo "✗ File $file has $stats lines changed (max allowed: 1)"
        all_good=false
    else
        echo "✓ File $file has $stats line(s) changed"
    fi
done

if [ "$all_good" = true ]; then
    echo "✓ All checks passed: only uv.lock files changed with at most 1 line each"
    exit 0
else
    exit 1
fi
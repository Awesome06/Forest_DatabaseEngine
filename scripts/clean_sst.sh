#!/bin/sh
# scripts/clean_sst.sh
# On-demand cleanup utility: Recursively purges all transient .sst database files
# across the repository and unstages any if indexed. Invoked via `make clean-sst`.

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$REPO_ROOT" || exit 0

echo "SST Clean Utility (POSIX): Scanning for transient .sst database files..."

COUNT=0
# Find and delete all .sst files recursively
if command -v find >/dev/null 2>&1; then
    COUNT=$(find . -name "*.sst" -type f | wc -l | tr -d ' ')
    find . -name "*.sst" -type f -delete 2>/dev/null || true
fi

# Unstage any .sst files that may have been staged in the git index
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    STAGED=$(git diff --cached --name-only -- "*.sst" 2>/dev/null)
    if [ -n "$STAGED" ]; then
        echo "Pre-Commit Guard: Unstaging accidentally staged .sst files..."
        git reset HEAD -- "*.sst" 2>/dev/null || true
    fi
fi

if [ "$COUNT" -gt 0 ]; then
    echo "SST Clean Utility: Successfully deleted $COUNT .sst file(s)."
else
    echo "SST Clean Utility: Working directory clean. Zero .sst files found."
fi

exit 0

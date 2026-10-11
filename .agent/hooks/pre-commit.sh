#!/bin/sh
# .agent/hooks/pre-commit.sh
# POSIX skill synchronization fallback for macOS, Linux, and DevContainers.

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
cd "$REPO_ROOT" || exit 0

REGISTRY_MAP=".agent/.registry/skill-map.json"
ACTIVE_SKILLS=".agent/.active-skills.json"
SKILLS_DIR=".agent/skills"

# Exit silently if vault isn't vendored yet
[ -f "$REGISTRY_MAP" ] || exit 0

mkdir -p "$SKILLS_DIR"

if command -v jq >/dev/null 2>&1; then
    for trigger in $(jq -r 'keys[]' "$REGISTRY_MAP"); do
        if [ -e "$trigger" ]; then
            for skill in $(jq -r --arg t "$trigger" '.[$t][]' "$REGISTRY_MAP"); do
                folder_name=$(basename "$skill")
                target="$REPO_ROOT/.agent/.registry/$skill"
                link="$SKILLS_DIR/$folder_name"
                if [ -d "$target" ] && [ ! -e "$link" ]; then
                    echo "Antigravity Sync (POSIX): Linking skill -> $folder_name ($skill)"
                    ln -s "$target" "$link"
                fi
            done
        fi
    done
fi

exit 0

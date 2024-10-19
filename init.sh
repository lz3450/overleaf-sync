#!/usr/bin/env bash

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <overleaf_project_path>"
    exit 1
fi

OVERLEAF_PROJECT_PATH="$1"

if [[ -d "$OVERLEAF_PROJECT_PATH" ]]; then
    echo "The directory \"$OVERLEAF_PROJECT_PATH\" already exists."
    exit 2
fi

mkdir -p "$OVERLEAF_PROJECT_PATH"/.overleaf-sync
ln -rsf overleaf_sync.py -t "$OVERLEAF_PROJECT_PATH"/.overleaf-sync/
cp config.example.json "$OVERLEAF_PROJECT_PATH"/.overleaf-sync/config.json
echo "*" > "$OVERLEAF_PROJECT_PATH"/.overleaf-sync/.gitignore

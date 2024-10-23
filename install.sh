#!/usr/bin/env bash

if [[ ! -d "$HOME/.local/bin" ]]; then
    mkdir -p "$HOME/.local/bin"
fi

ln -rsf overleaf_sync.py "$HOME/.local/bin/overleaf-sync"

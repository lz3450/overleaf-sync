#!/usr/bin/env bash

if [[ ! -d "$HOME/.local/bin" ]]; then
    mkdir -vp "$HOME/.local/bin"
fi

ln -vrsf overleaf_sync/overleaf_sync.py "$HOME/.local/lib/python3.14t/site-packages/overleaf_sync"

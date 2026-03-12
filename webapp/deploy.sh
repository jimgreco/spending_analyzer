#!/bin/bash
set -e
cd "$(dirname "$0")"

# Bake current git version into version.json so prod can read it
SHA=$(git rev-parse --short HEAD)
TS=$(git log -1 --format=%ci)
echo "{\"sha\": \"$SHA\", \"timestamp\": \"$TS\"}" > version.json

eb deploy

# Remove version.json locally (not needed, git is available locally)
rm version.json

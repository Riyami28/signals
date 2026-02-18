#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p data/local/postgres data/local/redis

docker compose -f docker-compose.local.yml up -d postgres redis huginn

echo "Local stack is starting."
docker compose -f docker-compose.local.yml ps

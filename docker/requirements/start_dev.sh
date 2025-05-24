#!/bin/bash

# Navigate to project root
cd "$(dirname "$0")/.."

# Load environment variables from .env
if [ ! -f ".env" ]; then
  echo "❌ .env file not found at project root"
  exit 1
fi

set -a
source .env
set +a

# Go back to docker directory and run docker compose
cd docker
docker compose -f docker-compose.yaml up --build dev_spg

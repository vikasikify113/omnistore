#!/usr/bin/env bash
# One command for hosting platforms (Render, Railway, Fly...): runs the three Kafka consumers
# in the background and the web app in the foreground.
set -e
python -m omnistore.setup --skip-kafka --skip-search >/dev/null 2>&1 || true   # make sure tables exist
python -m omnistore.consumers all &
exec uvicorn omnistore.api:app --host 0.0.0.0 --port "${PORT:-8000}"

#!/usr/bin/env bash
# Deploy: git pull, pip install, systemctl restart, curl health check.
set -e
cd "$(dirname "$0")"
git pull
pip install -e .
systemctl restart btc-kalshi.service
sleep 3
curl -sf http://127.0.0.1:8000/api/status > /dev/null && echo "Health check OK" || echo "Health check FAILED"

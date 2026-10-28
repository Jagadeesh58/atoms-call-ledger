#!/usr/bin/env bash
set -euo pipefail

# scripts/check_repo.sh - Secrets grep, large-file check, no .env or .db tracked
python scripts/check_repo.py

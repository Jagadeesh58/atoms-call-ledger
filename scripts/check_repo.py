#!/usr/bin/env python3
"""
WHY:
Repository hygiene and honesty check script.
Verifies no secrets, no AWS tokens, no forbidden databases (.db, .sqlite, .env),
no large files (> 1MB), and no non-synthetic phone numbers are tracked.
Cross-platform: works on Windows, Linux, and macOS.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys


def main() -> int:
    print("==> Running repo hygiene checks...")

    # 1. Check forbidden tracked files
    files = subprocess.check_output(["git", "ls-files"], text=True).splitlines()
    forbidden_patterns = [r"\.env($|\..*)", r".*\.db$", r".*\.sqlite.*", r"^spool\.jsonl$"]
    for f in files:
        if f == ".env.example":
            continue
        for pat in forbidden_patterns:
            if re.search(pat, f):
                print(f"ERROR: Forbidden file tracked by git: {f}")
                return 1

    # 2. Check large files (> 1MB)
    max_bytes = 1048576
    for f in files:
        if os.path.isfile(f):
            sz = os.path.getsize(f)
            if sz > max_bytes:
                print(f"ERROR: File exceeds 1MB: {f} ({sz} bytes)")
                return 1

    # 3. Check credentials and phone numbers
    sk_pattern = re.compile(r"sk-[a-zA-Z0-9_-]{20,}")
    akia_pattern = re.compile(r"AKIA[0-9A-Z]{16}")
    bearer_pattern = re.compile(r"Bearer [a-zA-Z0-9_\-\.]{25,}")
    phone_pattern = re.compile(r"\+[1-9][0-9]{9,14}")

    for f in files:
        if f in ("scripts/check_repo.sh", "scripts/check_repo.py"):
            continue
        if os.path.isfile(f):
            try:
                with open(f, "r", encoding="utf-8", errors="ignore") as fh:
                    content = fh.read()
            except Exception:
                continue

            if sk_pattern.search(content):
                print(f"ERROR: Potential API key found in {f}")
                return 1
            if akia_pattern.search(content):
                print(f"ERROR: Potential AWS key found in {f}")
                return 1
            if bearer_pattern.search(content):
                print(f"ERROR: Potential Bearer token found in {f}")
                return 1

            phones = phone_pattern.findall(content)
            for p in phones:
                if not p.startswith("+91000000"):
                    print(
                        f"ERROR: Real-looking phone number '{p}' found in {f} (must use +91000000xxxx)"
                    )
                    return 1

    print("==> All repo hygiene checks passed!")
    return 0


if __name__ == "__main__":
    sys.exit(main())

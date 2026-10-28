"""
WHY:
Vercel serverless entry point for the atoms-call-ledger web application.
Routes all requests to the FastAPI app defined in app/main.py.
This file is the single Vercel build target per vercel.json.
"""

import os
import sys

# Add src to path for library imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from app.main import app  # noqa: F401, E402 – re-export for Vercel ASGI handler

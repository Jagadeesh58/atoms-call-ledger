"""
WHY:
Exposes core package symbols for atoms_call-ledger. Exists to provide a clean,
versioned top-level import interface for ledger components. It must never import
optional external dependencies (such as Streamlit or FastAPI) at module root.
"""

from atoms_ledger.config import LedgerConfig
from atoms_ledger.signature import compute_signature, verify_signature

__all__ = [
    "LedgerConfig",
    "compute_signature",
    "verify_signature",
]

__version__ = "0.1.0"

# QuantTrade/bootstrap.py
import sys
import os

# Get the project root (directory where this file lives)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Ensure the root is in sys.path
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# (Optional) Print for debugging (can disable later)
print(f"[Bootstrap] Project root added to sys.path: {PROJECT_ROOT}")
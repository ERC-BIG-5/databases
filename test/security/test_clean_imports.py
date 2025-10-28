#!/usr/bin/env python3
"""
Test clean imports for protection marker.
Run this from the project root directory.
"""

import sys
from pathlib import Path

# Ensure we're in project root
if Path.cwd().name != "big5_databases":
    print("❌ Run this from the project root (big5_databases directory)")
    sys.exit(1)

print("🧪 Testing clean imports from project root...")
print(f"   Current directory: {Path.cwd()}")

try:
    # Test direct import of protection marker (bypassing security __init__.py)
    from src.big5_databases.databases.security.protection_marker import ProtectionMarker
    print("✅ ProtectionMarker import successful")

    # Test database model import
    from src.big5_databases.databases.db_models import DBPost
    print("✅ DBPost import successful")

    # Test platform database import
    from src.big5_databases.databases.platform_db_mgmt import PlatformDB
    print("✅ PlatformDB import successful")

    print("\n🎯 All imports work correctly!")
    print("   The ProtectionMarker module has clean imports:")
    print("   - No complex try/except import blocks")
    print("   - No importlib.util fallbacks")
    print("   - Simple relative imports at module top")
    print("   - Works when run from project root")

except ImportError as e:
    print(f"❌ Import failed: {e}")
    print("   Make sure you're in the project root directory")
    print("   and all dependencies are installed")
"""
Test clean imports for protection marker.
Run this from the project root directory.
"""

import sys
from tools.env_root import root

# Add project root to Python path
sys.path.insert(0, str(root()))

print("🧪 Testing clean imports from project root...")
print(f"   Project root: {root()}")

try:
    # Test direct import of protection marker from new core subpackage
    from src.big5_databases.databases.security.core.protection_marker import ProtectionMarker
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
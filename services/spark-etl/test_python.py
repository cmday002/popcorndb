#!/usr/bin/env python3
"""
Simple Python version test script
"""

import sys
import platform

def main():
    print("🐍 Python Version Test")
    print("=" * 30)
    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    print(f"Platform: {platform.platform()}")
    print(f"Architecture: {platform.architecture()}")
    
    # Test basic imports
    try:
        import pyspark
        print("✅ PySpark imported successfully")
    except ImportError as e:
        print(f"❌ PySpark import failed: {e}")
    
    try:
        import pandas
        print(f"✅ Pandas imported successfully (version: {pandas.__version__})")
    except ImportError as e:
        print(f"❌ Pandas import failed: {e}")
    
    try:
        import numpy
        print(f"✅ NumPy imported successfully (version: {numpy.__version__})")
    except ImportError as e:
        print(f"❌ NumPy import failed: {e}")
    
    print("=" * 30)
    print("Test completed!")

if __name__ == "__main__":
    main()

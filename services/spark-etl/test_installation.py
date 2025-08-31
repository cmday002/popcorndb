#!/usr/bin/env python3
"""
Test script to verify Spark ETL service installation
"""

import sys
import importlib

def test_imports():
    """Test that all required packages can be imported."""
    required_packages = [
        'pyspark',
        'pyiceberg', 
        'pandas',
        'numpy',
        'requests',
        'dotenv'
    ]
    
    print("🧪 Testing package imports...")
    
    failed_imports = []
    for package in required_packages:
        try:
            importlib.import_module(package)
            print(f"  ✅ {package}")
        except ImportError as e:
            print(f"  ❌ {package}: {e}")
            failed_imports.append(package)
    
    if failed_imports:
        print(f"\n❌ Failed to import: {', '.join(failed_imports)}")
        return False
    else:
        print("\n✅ All packages imported successfully!")
        return True

def test_spark_session():
    """Test that Spark session can be created."""
    try:
        from pyspark.sql import SparkSession
        
        print("\n🚀 Testing Spark session creation...")
        
        # Create a minimal Spark session
        spark = SparkSession.builder \
            .appName("Test-Session") \
            .master("local[1]") \
            .getOrCreate()
        
        print("  ✅ Spark session created successfully")
        
        # Test basic functionality
        test_df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "value"])
        count = test_df.count()
        print(f"  ✅ Test DataFrame created with {count} rows")
        
        # Clean up
        spark.stop()
        print("  ✅ Spark session stopped successfully")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Spark session test failed: {e}")
        return False

def main():
    """Main test function."""
    print("🎬 Spark ETL Service - Installation Test")
    print("=" * 50)
    
    # Test package imports
    imports_ok = test_imports()
    
    # Test Spark functionality
    spark_ok = test_spark_session()
    
    # Summary
    print("\n" + "=" * 50)
    if imports_ok and spark_ok:
        print("🎉 All tests passed! Installation is ready.")
        return 0
    else:
        print("❌ Some tests failed. Please check the installation.")
        return 1

if __name__ == "__main__":
    exit(main())

#!/bin/bash
# Startup script for Spark ETL service

set -e

echo "🚀 Starting Spark ETL Service..."

# Wait for Hive metastore to be ready
echo "⏳ Waiting for Hive metastore..."
until nc -z hive-metastore 9083; do
    echo "Hive metastore not ready, waiting..."
    sleep 5
done
echo "✅ Hive metastore is ready!"

# Wait for MySQL to be ready
echo "⏳ Waiting for MySQL..."
until nc -z mysql 3306; do
    echo "MySQL not ready, waiting..."
    sleep 5
done
echo "✅ MySQL is ready!"

# Ensure proper permissions on data directories
echo "🔧 Setting up data directories..."
mkdir -p /opt/data/raw /opt/data/processed /opt/data/warehouse

# Check if we need to fix permissions (run as root if needed)
if [ ! -w /opt/data/raw ]; then
    echo "⚠️  Permission issue detected, fixing..."
    # This will be handled by the container restart with proper permissions
    echo "Please restart the container to apply permission fixes"
    exit 1
fi

echo "✅ Data directories ready with proper permissions"

# Check if IMDb data exists, download if needed
if [ ! -f "/opt/data/raw/title.basics.tsv" ]; then
    echo "📥 IMDb data not found, downloading..."
    python3 /opt/spark-apps/scripts/download_imdb_data.py
else
    echo "✅ IMDb data already exists"
fi

echo "🎬 Spark ETL Service is ready!"
echo "Available commands:"
echo "  - Run ETL: spark-submit jobs/imdb_ingest.py"
echo "  - Check quality: spark-submit jobs/data_quality_check.py"
echo "  - Download data: python3 scripts/download_imdb_data.py"
echo "  - Test installation: python3 test_installation.py"

# Keep container running
exec "$@"

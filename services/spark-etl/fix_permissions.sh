#!/bin/bash
# Permission fix script for Spark ETL service

echo "🔧 Fixing permissions for Spark ETL service..."

# Check if container is running
if ! docker ps | grep -q "spark-etl"; then
    echo "❌ Spark ETL container is not running. Start it first:"
    echo "   cd infra && docker-compose up spark-etl -d"
    exit 1
fi

echo "✅ Container is running, fixing permissions..."

# Fix permissions on data directories
docker exec -u root spark-etl chown -R spark:spark /opt/data
docker exec -u root spark-etl chmod -R 755 /opt/data

# Verify the fix
echo "🔍 Verifying permissions..."
docker exec spark-etl ls -la /opt/data/

# Test write access
echo "🧪 Testing write access..."
if docker exec spark-etl touch /opt/data/raw/test_write.tmp; then
    echo "✅ Write access working!"
    docker exec spark-etl rm /opt/data/raw/test_write.tmp
else
    echo "❌ Write access still not working"
    exit 1
fi

echo "🎉 Permissions fixed successfully!"
echo ""
echo "Now you can run:"
echo "  docker exec spark-etl python3 scripts/download_imdb_data.py"
echo "  docker exec spark-etl spark-submit jobs/imdb_ingest.py"

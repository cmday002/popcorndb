#!/bin/bash
# Build script for Spark ETL service

set -e

echo "🔨 Building Spark ETL Docker image..."

# Try the main Dockerfile first
echo "📦 Attempting to build with main Dockerfile..."
if docker build -t spark-etl:latest .; then
    echo "✅ Main Dockerfile build successful!"
    
    # Test the built image
    echo "🧪 Testing the built image..."
    if docker run --rm spark-etl:latest python3 test_python.py; then
        echo "✅ Image test successful!"
    else
        echo "⚠️  Image test failed, but build succeeded"
    fi
    
else
    echo "⚠️  Main Dockerfile failed, trying alternative..."
    
    # Try the alternative Dockerfile
    if docker build -f Dockerfile.alternative -t spark-etl:latest .; then
        echo "✅ Alternative Dockerfile build successful!"
        
        # Test the built image
        echo "🧪 Testing the built image..."
        if docker run --rm spark-etl:latest python test_python.py; then
            echo "✅ Image test successful!"
        else
            echo "⚠️  Image test failed, but build succeeded"
        fi
        
    else
        echo "❌ Both Dockerfiles failed. Let's troubleshoot..."
        echo ""
        echo "🔍 Troubleshooting steps:"
        echo "1. Check available Spark images:"
        echo "   docker search apache/spark"
        echo ""
        echo "2. Check what Python versions are available:"
        echo "   docker run --rm apache/spark:3.5.0-python3 python3 --version"
        echo ""
        echo "3. Try building with specific tags:"
        echo "   docker build -t spark-etl:latest ."
        echo ""
        echo "4. Check Docker daemon is running:"
        echo "   docker info"
        echo ""
        echo "5. Check available disk space:"
        echo "   df -h"
        exit 1
    fi
fi

echo ""
echo "🎉 Docker image built successfully!"
echo ""
echo "🚀 To run the service:"
echo "  cd ../../infra"
echo "  docker-compose up --build"
echo ""
echo "🧪 To test the image:"
echo "  docker run -it --rm spark-etl:latest bash"
echo ""
echo "📊 To run ETL jobs:"
echo "  docker run -it --rm -v \$(pwd)/data:/opt/data spark-etl:latest spark-submit jobs/imdb_ingest.py"
echo ""
echo "🔍 To check what was built:"
echo "  docker images | grep spark-etl"
echo "  docker run --rm spark-etl:latest python3 --version"
echo "  docker run --rm spark-etl:latest java -version"
echo ""
echo "🐍 To test Python functionality:"
echo "  docker run --rm spark-etl:latest python3 test_python.py"

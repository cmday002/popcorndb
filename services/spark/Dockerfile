FROM bitnami/spark:3.5.1

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip list && \
    python -c "import requests; print('requests module found')"

# Download Iceberg JARs
RUN mkdir -p /opt/bitnami/spark/jars && \
    cd /opt/bitnami/spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.4.2/iceberg-hive-runtime-1.4.2.jar

# Set working directory
WORKDIR /opt/spark-apps

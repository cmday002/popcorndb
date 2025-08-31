FROM apache/spark:3.5.0-python3

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Check Python version and install compatible packages
RUN python3 --version

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /opt/spark-apps
WORKDIR /opt/spark-apps

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip"
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Create directories for data with proper permissions
RUN mkdir -p /opt/data/raw /opt/data/processed /opt/data/warehouse && \
    chown -R spark:spark /opt/data && \
    chmod -R 755 /opt/data

# Switch back to spark user
USER spark

CMD ["tail", "-f", "/dev/null"]

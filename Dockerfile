FROM flink:1.19-scala_2.12

# Install Python + pip
# todo these aren't all running for some reason
# need to shell into the container and run manually
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip install --no-cache-dir apache-flink==1.19.* && \
    pip install google && \
    pip install google-api-core
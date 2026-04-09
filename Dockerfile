# eclipse-temurin ships OpenJDK 17 + Python in one image, avoiding apt package issues
FROM eclipse-temurin:17-jdk-jammy

# Install Python 3.11
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.11 \
        python3.11-dev \
        python3-pip \
        procps \
    && ln -sf /usr/bin/python3.11 /usr/bin/python \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer cached unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source and config
COPY src/       ./src/
COPY config/    ./config/

# Data directories are mounted at runtime via docker-compose volumes
# so they are not baked into the image.

ENV PYTHONPATH="/app/src"

CMD ["python3", "/app/src/main.py", "--config", "/config/pipeline.json"]

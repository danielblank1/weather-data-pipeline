FROM python:3.11-slim

# Install Bruin CLI
RUN apt-get update && apt-get install -y curl && \
    curl -LsSf https://getbruin.com/install/cli | sh && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Add Bruin to PATH
ENV PATH="/root/.local/bin:${PATH}"

# Install Python dependencies for Bruin assets
COPY assets/ingestion/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app/bruin

# Default: validate and run the pipeline
ENTRYPOINT ["bruin"]
CMD ["run", "--environment", "docker"]

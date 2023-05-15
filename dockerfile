# Use the official Airflow image as the base
FROM apache/airflow:2.6.0

# Install Chrome and Chromedriver
USER root
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-chromedriver \
    && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/lib/chromium-browser/chromedriver /usr/bin/chromedriver

# Switch back to the airflow user
USER airflow

# Install additional Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

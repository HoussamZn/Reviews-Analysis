FROM python:3.10-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gnupg \
    curl \
    ca-certificates \
    fonts-liberation \
    libnss3 \
    libxss1 \
    libappindicator1 \
    libgbm-dev \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libx11-xcb1 \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome for Testing with Chromedriver (version pinned)
ENV CHROME_VERSION=122.0.6261.111

RUN wget -q "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${CHROME_VERSION}/linux64/chrome-linux64.zip" && \
    wget -q "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${CHROME_VERSION}/linux64/chromedriver-linux64.zip" && \
    unzip chrome-linux64.zip && \
    unzip chromedriver-linux64.zip && \
    mv chrome-linux64 /opt/chrome && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/ && \
    chmod +x /usr/local/bin/chromedriver && \
    ln -s /opt/chrome/chrome /usr/bin/google-chrome && \
    rm -rf *.zip chromedriver-linux64

# Set display port to avoid issues with headless Chrome
ENV DISPLAY=:99


# Set environment variables
ENV PATH="/usr/local/bin:$PATH"

# Create working directory
WORKDIR /app

# Copy files
COPY producer.py .
COPY review_scraper.py .
COPY requirements.txt .
# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the producer script
CMD ["python", "producer.py"]


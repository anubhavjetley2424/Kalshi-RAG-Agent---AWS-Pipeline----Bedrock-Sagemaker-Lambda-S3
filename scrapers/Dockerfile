FROM python:3.10-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget unzip gnupg2 curl ca-certificates \
    fonts-liberation libnss3 libx11-xcb1 libxcomposite1 \
    libxcursor1 libxdamage1 libxrandr2 libasound2 libatk1.0-0 \
    libatk-bridge2.0-0 libcups2 libdrm2 libgbm1 libpango-1.0-0 \
    libpangocairo-1.0-0 libgtk-3-0 libxshmfence1 libxext6 libxfixes3 \
    libxrender1 libxinerama1 libxi6 libglib2.0-0 libfontconfig1 \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN wget -q https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_140.0.7339.207-1_amd64.deb \
    && dpkg -i google-chrome-stable_140.0.7339.207-1_amd64.deb || apt-get install -f -y \
    && rm google-chrome-stable_140.0.7339.207-1_amd64.deb

# Install matching ChromeDriver via Chrome for Testing
RUN wget -q https://storage.googleapis.com/chrome-for-testing-public/140.0.7339.207/linux64/chromedriver-linux64.zip  \
    && unzip chromedriver-linux64.zip \
    && mv chromedriver-linux64/chromedriver /usr/local/bin/ \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf chromedriver-linux64 chromedriver-linux64.zip

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Default command
CMD ["python", "main.py"]
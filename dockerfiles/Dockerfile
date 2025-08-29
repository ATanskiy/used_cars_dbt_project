# Use slim Python 3.12 as base
FROM python:3.12-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies (needed for psycopg2 and some Python libs)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (so Docker can cache pip layer)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your project code
COPY . .

# Default command (keeps container alive and ready)
CMD ["tail", "-f", "/dev/null"]
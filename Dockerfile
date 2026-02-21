FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install wgrib2 + minimal utilities
# If this fails on your first Render build due to package availability, tell me the exact error and
# I'll give you a fallback Dockerfile (Ubuntu base or source build).
RUN apt-get update && apt-get install -y --no-install-recommends \
    wgrib2 \
    ca-certificates \
    curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000

# Render injects $PORT. Use shell form so env var expands.
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
FROM condaforge/miniforge3:latest

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python + wgrib2 from conda-forge (reliable for Linux)
RUN conda install -y -c conda-forge python=3.11 wgrib2 pip \
 && conda clean -afy

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000

# Render injects $PORT
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]

FROM public.ecr.aws/docker/library/python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install system packages (include Tesseract OCR if PDF OCR is used)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    tesseract-ocr \
    libtesseract-dev \
    libgl1 \
    libglib2.0-0 \
    curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (leverage Docker layer caching)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Optional: install additional PDF loader requirements if present
COPY pdf_loader/requirements.txt ./pdf_loader/requirements.txt
RUN if [ -f pdf_loader/requirements.txt ]; then pip install --no-cache-dir -r pdf_loader/requirements.txt; fi

# Copy application code
COPY . .

EXPOSE 8080

# Streamlit server options: bind to 0.0.0.0:8080, disable CORS/XSRF for proxying
CMD ["streamlit", "run", "streamlit_app.py", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false", \
     "--server.port=8080", \
     "--server.address=0.0.0.0"]



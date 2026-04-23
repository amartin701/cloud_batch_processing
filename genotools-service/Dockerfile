FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    g++ \
    gcc \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY src/ /app/
RUN pip install -e .

RUN pip install --timeout 300 --retries 5 psutil flask prometheus-client pymongo

COPY genotools-service.py /app/

ENV NUMBA_NUM_THREADS=1
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1

CMD ["python", "genotools-service.py"]
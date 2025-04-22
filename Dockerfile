FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    apt-get remove -y gcc python3-dev libpq-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY consumer.py .

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s \
  CMD curl --fail http://localhost:8080/health || exit 1

CMD ["python", "consumer.py"]

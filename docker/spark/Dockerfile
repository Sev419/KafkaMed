FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV SPARK_MASTER=local[1]
ENV SPARK_SHUFFLE_PARTITIONS=1
ENV SPARK_DEFAULT_PARALLELISM=1
ENV SPARK_UI_ENABLED=false
ENV SPARK_DRIVER_BIND_ADDRESS=127.0.0.1
ENV SPARK_DRIVER_HOST=127.0.0.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "spark_processing/jobs/smoke_spark_session.py"]

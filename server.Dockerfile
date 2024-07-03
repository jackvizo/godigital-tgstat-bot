FROM prefecthq/prefect:2.16-python3.11-conda

RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    pip install psycopg2-binary && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
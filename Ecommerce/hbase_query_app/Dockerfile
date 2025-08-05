FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY hbase_queries.py .
COPY kz_cleaned.csv .

CMD ["python", "hbase_queries.py"]

FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY hbase_queries.py .
COPY datos_kaggle/kz_cleaned.csv .

CMD ["python", "hbase_queries.py"]


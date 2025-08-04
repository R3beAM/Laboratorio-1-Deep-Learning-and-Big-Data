FROM python:3.10
WORKDIR /app
COPY analytics_queries.py /app/
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY consumer/consumer.py .
CMD ["python", "consumer.py"]

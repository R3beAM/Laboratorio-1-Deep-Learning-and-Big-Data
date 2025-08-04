FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY consumer/consumer.py .
CMD ["python", "consumer.py"]

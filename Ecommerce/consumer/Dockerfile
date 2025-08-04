FROM python:3.10-slim

WORKDIR /app

# Copiamos el requirements general desde la raíz
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código del consumer (incluye analytics_queries.py si está)
COPY . /app

CMD ["python", "consumer.py"]



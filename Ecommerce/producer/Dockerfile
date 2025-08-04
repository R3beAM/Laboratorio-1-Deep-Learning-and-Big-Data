FROM python:3.10-slim

WORKDIR /app

# Copiamos el requirements general desde la raíz
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código del producer
COPY . /app

CMD ["python", "producer.py"]



FROM python:3.10-slim

WORKDIR /app

# Copiamos el requirements.txt desde la raíz del proyecto
COPY requirements.txt /app/requirements.txt

# Instalamos dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el script del consumidor desde la subcarpeta consumer/
COPY consumer/consumer.py /app/consumer.py

# Comando por defecto
CMD ["python", "consumer.py"]


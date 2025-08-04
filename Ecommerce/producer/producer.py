FROM python:3.10-slim

WORKDIR /app

# Copiar requirements.txt desde el contexto raíz (.)
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# 👇 Esta es la línea CLAVE: copiar desde la subcarpeta `producer/`
COPY producer/producer.py /app/producer.py

CMD ["python", "producer.py"]



FROM python:3.10-slim

WORKDIR /app

# Copiar el requirements.txt desde la raíz del proyecto
COPY requirements.txt /app/requirements.txt

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el archivo producer.py desde la carpeta producer/
COPY producer/producer.py /app/producer.py

# Comando por defecto
CMD ["python", "producer.py"]


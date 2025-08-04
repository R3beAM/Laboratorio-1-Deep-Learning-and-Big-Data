FROM python:3.10-slim

WORKDIR /app

# Copiar requirements y luego instalar (para cacheo eficiente)
COPY consumer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el código de consumer (incluye analytics_queries.py)
COPY consumer /app

CMD ["python", "consumer.py"]  # default, se sobreescribe en analytics_queries

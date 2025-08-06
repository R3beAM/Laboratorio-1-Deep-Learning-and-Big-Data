import pandas as pd
from redis import Redis
from dateutil import parser
from collections import defaultdict
import os

# Conexión a Redis
redis = Redis(host='localhost', port=6379, decode_responses=True)

# Ruta del CSV
CSV_PATH = os.path.join('datos_kaggle', 'kz_cleaned.csv')

# Cargar datos
df = pd.read_csv(CSV_PATH)

# Limpieza básica y transformación
df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
df['month'] = df['datetime'].dt.to_period('M').astype(str)
df['revenue'] = df['price'] * df['quantity']

# --- Almacenamiento en Redis ---
# Usaremos Redis Hashes para almacenar agregados

# Borramos claves previas (cuidado si usas Redis en producción)
redis.delete('category_sales', 'brand_revenue', 'monthly_sales')

# Agregamos a Redis
category_sales = defaultdict(int)
brand_revenue = defaultdict(float)
monthly_sales = defaultdict(float)

for _, row in df.iterrows():
    category = row['category']
    brand = row['brand']
    month = row['month']
    quantity = row['quantity']
    revenue = row['revenue']

    category_sales[category] += quantity
    brand_revenue[brand] += revenue
    monthly_sales[month] += revenue

# Guardar en Redis
redis.hset('category_sales', mapping=category_sales)
redis.hset('brand_revenue', mapping=brand_revenue)
redis.hset('monthly_sales', mapping=monthly_sales)

# --- Consultas ---

def get_max_from_hash(hash_name):
    data = redis.hgetall(hash_name)
    max_key = max(data, key=lambda k: float(data[k]))
    return max_key, float(data[max_key])

# Consulta 1: Categoría más vendida
cat, qty = get_max_from_hash('category_sales')
print(f'📊 Categoría más vendida: {cat} con {qty} unidades')

# Consulta 2: Marca con más ingresos brutos
brand, revenue = get_max_from_hash('brand_revenue')
print(f'🏷️ Marca con más ingresos: {brand} con ${revenue:,.2f}')

# Consulta 3: Mes con más ventas (en UTC)
month, rev = get_max_from_hash('monthly_sales')
print(f'🗓️ Mes con más ventas: {month} con ${rev:,.2f}')

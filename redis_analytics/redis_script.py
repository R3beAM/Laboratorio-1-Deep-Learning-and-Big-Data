import pandas as pd
from redis import Redis
from collections import defaultdict
import os

# Conexión a Redis
redis = Redis(host='localhost', port=6379, decode_responses=True)

# Ruta al archivo CSV
CSV_PATH = os.path.join('datos_kaggle', 'kz_cleaned.csv')

# Cargar el CSV
df = pd.read_csv(CSV_PATH)

# Normalizar nombres de columnas
df.columns = df.columns.str.strip().str.lower()

# Validación básica
if 'event_time' not in df.columns or 'category_id' not in df.columns or 'brand' not in df.columns or 'price' not in df.columns:
    raise ValueError("❌ El CSV no contiene las columnas necesarias: event_time, category_id, brand, price")

# Procesar fechas
df['event_time'] = pd.to_datetime(df['event_time'], utc=True)
df['month'] = df['event_time'].dt.to_period('M').astype(str)

# Suponemos 1 unidad vendida por fila
df['quantity'] = 1
df['revenue'] = df['price'] * df['quantity']

# --- Almacenamiento en Redis ---
redis.delete('category_sales', 'brand_revenue', 'monthly_sales')

category_sales = defaultdict(int)
brand_revenue = defaultdict(float)
monthly_sales = defaultdict(float)

for _, row in df.iterrows():
    category = row['category_id']
    brand = row['brand']
    month = row['month']
    quantity = row['quantity']
    revenue = row['revenue']

    if pd.isna(category) or pd.isna(brand):
        continue  # Saltar filas incompletas

    category_sales[str(category)] += quantity
    brand_revenue[brand] += revenue
    monthly_sales[month] += revenue

# Guardar en Redis
redis.hset('category_sales', mapping=category_sales)
redis.hset('brand_revenue', mapping=brand_revenue)
redis.hset('monthly_sales', mapping=monthly_sales)

# --- Consultas ---

def get_max_from_hash(hash_name):
    data = redis.hgetall(hash_name)
    if not data:
        return None, 0
    max_key = max(data, key=lambda k: float(data[k]))
    return max_key, float(data[max_key])

# Consulta 1: Categoría más vendida
cat, qty = get_max_from_hash('category_sales')
print(f'📊 Categoría más vendida (ID): {cat} con {qty} unidades')

# Consulta 2: Marca con más ingresos brutos
brand, revenue = get_max_from_hash('brand_revenue')
print(f'🏷️ Marca con más ingresos: {brand} con ${revenue:,.2f}')

# Cons

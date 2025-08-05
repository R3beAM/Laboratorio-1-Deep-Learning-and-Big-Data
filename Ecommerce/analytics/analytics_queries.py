import os
import pymongo
import redis
import happybase
from datetime import datetime
from collections import defaultdict, Counter

# --- Configuración de conexiones ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
HBASE_HOST = os.getenv("HBASE_HOST", "hbase")

# --- Conexión a MongoDB ---
mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_db = mongo_client["ecommerce"]
mongo_collection = mongo_db["ventas"]

# --- Conexión a Redis ---
redis_client = redis.StrictRedis(host=REDIS_HOST, port=6379, decode_responses=True)

# --- Conexión a HBase ---
hbase = happybase.Connection(host=HBASE_HOST, port=9090)
hbase.open()
hbase_table = hbase.table("ventas")  # Asegúrate que esta tabla exista y tenga data


# =======================
# --- CONSULTAS ---
# =======================

print("---- CONSULTAS EN MONGODB ----")

# 1. ¿Cuál es la categoría más vendida?
most_sold_category = mongo_collection.aggregate([
    {"$group": {"_id": "$category", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
])
for doc in most_sold_category:
    print("Categoría más vendida:", doc["_id"], "-", doc["count"], "ventas")

# 2. ¿Qué marca generó más ingresos brutos?
top_brand = mongo_collection.aggregate([
    {"$group": {"_id": "$brand", "total_ingresos": {"$sum": "$price"}}},
    {"$sort": {"total_ingresos": -1}},
    {"$limit": 1}
])
for doc in top_brand:
    print("Marca con más ingresos:", doc["_id"], "-", round(doc["total_ingresos"], 2), "USD")

# 3. ¿Qué mes tuvo más ventas? (en UTC)
ventas_por_mes = mongo_collection.aggregate([
    {"$group": {
        "_id": {"$substr": ["$event_time", 0, 7]},  # YYYY-MM
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1}},
    {"$limit": 1}
])
for doc in ventas_por_mes:
    print("Mes con más ventas:", doc["_id"], "-", doc["count"], "ventas")


print("\n---- CONSULTAS EN REDIS ----")
# Redis suele almacenar datos como clave-valor, asumiremos claves tipo venta:<id>
# Esto requiere que hayas insertado datos previamente con claves significativas

ventas_keys = redis_client.keys("venta:*")

categorias = Counter()
brands = defaultdict(float)
meses = Counter()

for key in ventas_keys:
    venta = redis_client.hgetall(key)
    if venta:
        categorias[venta.get("category", "")] += 1
        brands[venta.get("brand", "")] += float(venta.get("price", 0))
        fecha = venta.get("event_time", "")
        if fecha:
            mes = fecha[:7]
            meses[mes] += 1

if categorias:
    print("Categoría más vendida:", categorias.most_common(1)[0])
if brands:
    top_brand = max(brands.items(), key=lambda x: x[1])
    print("Marca con más ingresos:", top_brand[0], "-", round(top_brand[1], 2), "USD")
if meses:
    top_month = meses.most_common(1)[0]
    print("Mes con más ventas:", top_month[0], "-", top_month[1], "ventas")


print("\n---- CONSULTAS EN HBASE ----")
# Suponemos que las columnas están en 'info:category', 'info:brand', 'info:price', 'info:event_time'

categorias_hb = Counter()
brands_hb = defaultdict(float)
meses_hb = Counter()

for key, data in hbase_table.scan():
    category = data.get(b'info:category', b'').decode()
    brand = data.get(b'info:brand', b'').decode()
    price = float(data.get(b'info:price', b'0').decode())
    event_time = data.get(b'info:event_time', b'').decode()

    categorias_hb[category] += 1
    brands_hb[brand] += price
    if event_time:
        mes = event_time[:7]
        meses_hb[mes] += 1

if categorias_hb:
    print("Categoría más vendida:", categorias_hb.most_common(1)[0])
if brands_hb:
    top_brand = max(brands_hb.items(), key=lambda x: x[1])
    print("Marca con más ingresos:", top_brand[0], "-", round(top_brand[1], 2), "USD")
if meses_hb:
    top_month = meses_hb.most_common(1)[0]
    print("Mes con más ventas:", top_month[0], "-", top_month[1], "ventas")

import os
import pandas as pd
import redis
import json
import time
import happybase
import time
from pymongo import MongoClient

csv_path = os.getenv("CSV_PATH", "/data/kz_cleaned.csv")
redis_client = redis.Redis(host='redis_alt', port=6379)

def stream_data():
    df = pd.read_csv(csv_path, chunksize=1000)
    for chunk in df:
        for _, row in chunk.iterrows():
            data = row.to_dict()
            redis_client.rpush("kz_queue", json.dumps(data))
        print("Batch pushed to Redis")
        time.sleep(0.5)

if __name__ == "__main__":
    stream_data()


# --- Espera inicial para asegurar que los servicios estén listos ---
time.sleep(10)

# --- MongoDB ---
mongo_client = MongoClient("mongodb://mongo:27017")
mongo_db = mongo_client["ecommerce"]
mongo_collection = mongo_db["ventas"]
mongo_collection.delete_many({})  # Limpia colección para evitar duplicados

# --- Redis ---
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
redis_client.flushdb()  # Limpia Redis

# --- HBase ---
hbase_conn = happybase.Connection(host="hbase", port=9090)
hbase_conn.open()

if b'ventas' not in hbase_conn.tables():
    hbase_conn.create_table(
        b'ventas',
        {b'info': dict()}
    )
hbase_table = hbase_conn.table('ventas')

# --- Inserta datos ---
for i, row in df.iterrows():
    doc = {
        "category": row["category"],
        "brand": row["brand"],
        "price": float(row["price"]),
        "event_time": row["event_time"]
    }

    # MongoDB
    mongo_collection.insert_one(doc)

    # Redis
    redis_key = f"venta:{i}"
    redis_client.hset(redis_key, mapping=doc)

    # HBase (todas las columnas dentro de "info")
    hbase_table.put(f"row{i}", {
        b'info:category': str(row["category"]).encode(),
        b'info:brand': str(row["brand"]).encode(),
        b'info:price': str(row["price"]).encode(),
        b'info:event_time': str(row["event_time"]).encode()
    })

print("✅ Datos cargados en MongoDB, Redis y HBase correctamente.")

import os
from pymongo import MongoClient
import redis
import happybase
import json
from collections import defaultdict
from datetime import datetime
from dateutil import parser
import pytz

# --- Conexión a MongoDB ---
mongo = MongoClient("mongodb://localhost:27017")
mongo_db = mongo["ecommerce"]
mongo_col = mongo_db["purchase_history"]

# --- Conexión a Redis ---
redis_client = redis.Redis(host="localhost", port=6379)

# --- Conexión a HBase ---
hbase = happybase.Connection(host="localhost", port=9090)
hbase_table = hbase.table("purchase_history")

# ===============================
#         MONGO QUERIES
# ===============================

def mongo_queries():
    print("\n--- MongoDB ---")

    # 1. Categoría más vendida
    top_category = mongo_col.aggregate([
        {"$group": {"_id": "$category_code", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}, {"$limit": 1}
    ])
    print("Categoría más vendida:", list(top_category)[0])

    # 2. Marca con más ingresos brutos
    top_brand = mongo_col.aggregate([
        {"$group": {"_id": "$brand", "total": {"$sum": "$price"}}},
        {"$sort": {"total": -1}}, {"$limit": 1}
    ])
    print("Marca con más ingresos:", list(top_brand)[0])

    # 3. Mes con más ventas (UTC)
    top_month = mongo_col.aggregate([
        {"$addFields": {
            "month": {"$dateToString": {"format": "%Y-%m", "date": {"$toDate": "$event_time"}}}
        }},
        {"$group": {"_id": "$month", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}, {"$limit": 1}
    ])
    print("Mes con más ventas:", list(top_month)[0])

# ===============================
#         REDIS QUERIES
# ===============================

def redis_queries():
    print("\n--- Redis ---")

    keys = redis_client.keys("processed:*")  # Suponiendo que almacenaste los datos con claves "processed:<id>"
    if not keys:
        print("No hay datos analíticos en Redis.")
        return

    category_counter = defaultdict(int)
    brand_revenue = defaultdict(float)
    month_sales = defaultdict(int)

    for key in keys:
        data = redis_client.get(key)
        if not data:
            continue
        record = json.loads(data)

        category = record.get("category_code")
        brand = record.get("brand")
        price = float(record.get("price", 0))
        time_str = record.get("event_time")

        category_counter[category] += 1
        brand_revenue[brand] += price

        if time_str:
            dt = parser.parse(time_str).astimezone(pytz.utc)
            month = dt.strftime("%Y-%m")
            month_sales[month] += 1

    print("Categoría más vendida:", max(category_counter, key=category_counter.get))
    print("Marca con más ingresos:", max(brand_revenue, key=brand_revenue.get))
    print("Mes con más ventas:", max(month_sales, key=month_sales.get))

# ===============================
#         HBASE QUERIES
# ===============================

def hbase_queries():
    print("\n--- HBase ---")

    category_counter = defaultdict(int)
    brand_revenue = defaultdict(float)
    month_sales = defaultdict(int)

    for key, data in hbase_table.scan():
        row = {k.decode().split(":")[1]: v.decode() for k, v in data.items()}

        category = row.get("category_code")
        brand = row.get("brand")
        price = float(row.get("price", 0))
        time_str = row.get("event_time")

        category_counter[category] += 1
        brand_revenue[brand] += price

        if time_str:
            dt = parser.parse(time_str).astimezone(pytz.utc)
            month = dt.strftime("%Y-%m")
            month_sales[month] += 1

    print("Categoría más vendida:", max(category_counter, key=category_counter.get))
    print("Marca con más ingresos:", max(brand_revenue, key=brand_revenue.get))
    print("Mes con más ventas:", max(month_sales, key=month_sales.get))


# ===============================
#             MAIN
# ===============================

if __name__ == "__main__":
    mongo_queries()
    redis_queries()
    hbase_queries()

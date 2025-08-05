from pymongo import MongoClient
import redis
import happybase
from collections import defaultdict
from datetime import datetime
import json

# ----------------------
# MongoDB
# ----------------------
def query_mongodb():
    client = MongoClient("mongodb://mongo:27017/")
    db = client["ecommerce"]
    collection = db["purchases"]

    print("\nMongoDB:")

    # 1. Categoría más vendida
    result = collection.aggregate([
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ])
    print("Categoría más vendida:", list(result)[0])

    # 2. Marca con más ingresos
    result = collection.aggregate([
        {"$group": {"_id": "$brand", "total": {"$sum": "$price"}}},
        {"$sort": {"total": -1}}, {"$limit": 1}
    ])
    print("Marca con más ingresos brutos:", list(result)[0])

    # 3. Mes con más ventas (UTC)
    result = collection.aggregate([
        {"$project": {
            "month": {"$substr": ["$event_time", 0, 7]}
        }},
        {"$group": {"_id": "$month", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}, {"$limit": 1}
    ])
    print("Mes con más ventas:", list(result)[0])


# ----------------------
# Redis
# ----------------------
def query_redis():
    r = redis.Redis(host="redis_alt", port=6379, decode_responses=True)
    keys = r.keys("purchase:*")

    category_counter = defaultdict(int)
    brand_revenue = defaultdict(float)
    month_counter = defaultdict(int)

    for key in keys:
        data = r.hgetall(key)
        if not data: continue

        category = data.get("category")
        brand = data.get("brand")
        price = float(data.get("price", 0.0))
        timestamp = data.get("event_time")

        if category: category_counter[category] += 1
        if brand: brand_revenue[brand] += price
        if timestamp:
            try:
                month = datetime.fromisoformat(timestamp).strftime("%Y-%m")
                month_counter[month] += 1
            except:
                continue

    print("\nRedis:")
    print("Categoría más vendida:", max(category_counter, key=category_counter.get))
    print("Marca con más ingresos brutos:", max(brand_revenue, key=brand_revenue.get))
    print("Mes con más ventas:", max(month_counter, key=month_counter.get))


# ----------------------
# HBase
# ----------------------
def query_hbase():
    connection = happybase.Connection('hbase_alt')
    table = connection.table('purchases')

    category_counter = defaultdict(int)
    brand_revenue = defaultdict(float)
    month_counter = defaultdict(int)

    for key, data in table.scan():
        category = data.get(b"cf:category", b"").decode()
        brand = data.get(b"cf:brand", b"").decode()
        price = float(data.get(b"cf:price", b"0").decode())
        timestamp = data.get(b"cf:event_time", b"").decode()

        if category: category_counter[category] += 1
        if brand: brand_revenue[brand] += price
        if timestamp:
            try:
                month = datetime.fromisoformat(timestamp).strftime("%Y-%m")
                month_counter[month] += 1
            except:
                continue

    print("\nHBase:")
    print("Categoría más vendida:", max(category_counter, key=category_counter.get))
    print("Marca con más ingresos brutos:", max(brand_revenue, key=brand_revenue.get))
    print("Mes con más ventas:", max(month_counter, key=month_counter.get))


# ----------------------
# Ejecutar
# ----------------------
if __name__ == "__main__":
    query_mongodb()
    query_redis()
    query_hbase()

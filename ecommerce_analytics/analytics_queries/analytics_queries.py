from pymongo import MongoClient
import redis
import happybase
import pandas as pd
from collections import Counter
from datetime import datetime

# MongoDB
mongo_client = MongoClient("mongodb://mongo:27017/")
db = mongo_client["ecommerce"]
collection = db["sales"]

# Redis
redis_client = redis.Redis(host='redis_alt', port=6379, decode_responses=True)

# HBase
hbase_connection = happybase.Connection('hbase_alt')
table = hbase_connection.table('sales')

# Consulta 1: Categoría más vendida
def categoria_mas_vendida_mongo():
    result = collection.aggregate([
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}, {"$limit": 1}
    ])
    return list(result)[0]["_id"]

def categoria_mas_vendida_redis():
    keys = redis_client.keys("sale:*")
    counter = Counter()
    for key in keys:
        category = redis_client.hget(key, "category")
        if category:
            counter[category] += 1
    return counter.most_common(1)[0][0]

def categoria_mas_vendida_hbase():
    counter = Counter()
    for _, data in table.scan():
        category = data.get(b'data:category')
        if category:
            counter[category.decode()] += 1
    return counter.most_common(1)[0][0]

# Consulta 2: Marca con más ingresos brutos
def marca_mas_ingresos_mongo():
    result = collection.aggregate([
        {"$group": {"_id": "$brand", "revenue": {"$sum": "$price"}}},
        {"$sort": {"revenue": -1}}, {"$limit": 1}
    ])
    return list(result)[0]["_id"]

def marca_mas_ingresos_redis():
    keys = redis_client.keys("sale:*")
    revenues = {}
    for key in keys:
        brand = redis_client.hget(key, "brand")
        price = redis_client.hget(key, "price")
        if brand and price:
            revenues[brand] = revenues.get(brand, 0) + float(price)
    return max(revenues, key=revenues.get)

def marca_mas_ingresos_hbase():
    revenues = {}
    for _, data in table.scan():
        brand = data.get(b'data:brand')
        price = data.get(b'data:price')
        if brand and price:
            brand = brand.decode()
            revenues[brand] = revenues.get(brand, 0) + float(price.decode())
    return max(revenues, key=revenues.get)

# Consulta 3: Mes con más ventas (UTC)
def mes_mas_ventas_mongo():
    result = collection.aggregate([
        {"$project": {"month": {"$substr": ["$event_time", 0, 7]}}},
        {"$group": {"_id": "$month", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}, {"$limit": 1}
    ])
    return list(result)[0]["_id"]

def mes_mas_ventas_redis():
    keys = redis_client.keys("sale:*")
    counter = Counter()
    for key in keys:
        time = redis_client.hget(key, "event_time")
        if time:
            month = time[:7]
            counter[month] += 1
    return counter.most_common(1)[0][0]

def mes_mas_ventas_hbase():
    counter = Counter()
    for _, data in table.scan():
        time = data.get(b'data:event_time')
        if time:
            month = time.decode()[:7]
            counter[month] += 1
    return counter.most_common(1)[0][0]

# Mostrar resultados
print("[MongoDB]")
print("Categoría más vendida:", categoria_mas_vendida_mongo())
print("Marca con más ingresos:", marca_mas_ingresos_mongo())
print("Mes con más ventas:", mes_mas_ventas_mongo())

print("\n[Redis]")
print("Categoría más vendida:", categoria_mas_vendida_redis())
print("Marca con más ingresos:", marca_mas_ingresos_redis())
print("Mes con más ventas:", mes_mas_ventas_redis())

print("\n[HBase]")
print("Categoría más vendida:", categoria_mas_vendida_hbase())
print("Marca con más ingresos:", marca_mas_ingresos_hbase())
print("Mes con más ventas:", mes_mas_ventas_hbase())

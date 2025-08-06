from pymongo import MongoClient
from collections import Counter
from datetime import datetime

# Conexión a MongoDB
client = MongoClient("mongodb://mongodb:27017/")
db = client.kz
collection = db.ventas

# ------------------------------------------------------------------------------
# 1️⃣ ¿Cuál es la categoría más vendida?
# MongoDB Query:
# db.ventas.aggregate([
#   { $match: { category_code: { $ne: null } } },
#   { $group: { _id: "$category_code", total: { $sum: 1 } } },
#   { $sort: { total: -1 } },
#   { $limit: 1 }
# ])
# ------------------------------------------------------------------------------

categorias = collection.aggregate([
    {"$match": {"category_code": {"$ne": None}}},
    {"$group": {"_id": "$category_code", "total": {"$sum": 1}}},
    {"$sort": {"total": -1}},
    {"$limit": 1}
])
categoria_mas_vendida = next(categorias, {}).get("_id", "No encontrada")
print(f"✅ Categoría más vendida: {categoria_mas_vendida}")

# ------------------------------------------------------------------------------
# 2️⃣ ¿Qué marca generó más ingresos brutos?
# MongoDB Query:
# db.ventas.aggregate([
#   { $match: { brand: { $ne: null } } },
#   { $group: { _id: "$brand", total_ingresos: { $sum: "$price" } } },
#   { $sort: { total_ingresos: -1 } },
#   { $limit: 1 }
# ])
# ------------------------------------------------------------------------------

marcas = collection.aggregate([
    {"$match": {"brand": {"$ne": None}}},
    {"$group": {"_id": "$brand", "total_ingresos": {"$sum": "$price"}}},
    {"$sort": {"total_ingresos": -1}},
    {"$limit": 1}
])
marca_top = next(marcas, {})
print(f"✅ Marca con más ingresos brutos: {marca_top.get('_id', 'No encontrada')} (${marca_top.get('total_ingresos', 0):,.2f})")

# ------------------------------------------------------------------------------
# 3️⃣ ¿Qué mes tuvo más ventas? (en UTC)
# MongoDB Query:
# db.ventas.aggregate([
#   {
#     $project: {
#       month: { $dateToString: { format: "%Y-%m", date: { $toDate: "$event_time" } } }
#     }
#   },
#   {
#     $group: {
#       _id: "$month",
#       total_ventas: { $sum: 1 }
#     }
#   },
#   { $sort: { total_ventas: -1 } },
#   { $limit: 1 }
# ])
# ------------------------------------------------------------------------------

ventas_por_mes = collection.aggregate([
    {
        "$project": {
            "month": {"$dateToString": {"format": "%Y-%m", "date": {"$toDate": "$event_time"}}}
        }
    },
    {
        "$group": {
            "_id": "$month",
            "total_ventas": {"$sum": 1}
        }
    },
    {"$sort": {"total_ventas": -1}},
    {"$limit": 1}
])
mes_top = next(ventas_por_mes, {}).get("_id", "No encontrado")
print(f"✅ Mes con más ventas (UTC): {mes_top}")

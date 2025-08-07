import pandas as pd
import pymongo
from pymongo import MongoClient


#Primer paso: Cargar csv
df = pd.read_csv('kz.csv')
print("\n✅ Primeras filas del dataset:\n")
print(df.head())

# Convertir la primera columna (event_time) a formato datetime
df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])



#Conectar a la base de datos
client = pymongo.MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)


#Crear la coleccion e insertamos los datos
db = client['kz_database']
collection = db['kz_collection']
if collection.count_documents({}) == 0:
    collection.insert_many(df.to_dict(orient='records'))
    print("✅ Datos insertados en MongoDB")
else:
    print("ℹ️ Los datos ya estaban insertados")

#-------------------------------------------------------------------------    
#Consultar los datos
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
resultado = collection.aggregate([
    {"$match": {"category_code": {"$ne": None}}},
    {"$group": {"_id": "$category_code", "total": {"$sum": 1}}},
    {"$sort": {"total": -1}},
    {"$limit": 1}
])
for doc in resultado:
    print(f"Categoría más vendida: {doc['_id']} con {doc['total']} ventas")
# ------------------------------------------------------------------------------
# 2️⃣ ¿Qué marca generó más ingresos brutos?
# MongoDB Query:
# db.ventas.aggregate([
#   { $match: { brand: { $ne: null } } },
#   { $group: { _id: "$brand", total_ingresos: { $sum: "$price" } } },
#   { $sort: { total_ingresos: -1 } },
#   { $limit: 1 }
# ])
# -------------------------------------------------------------
resultado = collection.aggregate([
    {"$match": {"brand": {"$ne": None}}},
    {"$group": {"_id": "$brand", "total_ingresos": {"$sum": "$price"}}},
    {"$sort": {"total_ingresos": -1}},
    {"$limit": 1}
])
for doc in resultado:
    print(f"Marca con más ingresos: {doc['_id']} con ${doc['total_ingresos']} ventas")

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
# event_time ya está en formato datetime, se extraen mes y año
# ------------------------------------------------------------------------------
resultado = collection.aggregate([
    {
        "$project": {
            "mes": {"$month": "$event_time"},
            "anio": {"$year": "$event_time"}
        }
    },
    {
        "$group": {
            "_id": {"anio": "$anio", "mes": "$mes"},
            "total_ventas": {"$sum": 1}
        }
    },
    {"$sort": {"total_ventas": -1}},
    {"$limit": 1}
])

for doc in resultado:
    print(f"Mes con más ventas: {doc['_id']['mes']}/{doc['_id']['anio']} con {doc['total_ventas']} ventas")
    
 
client.close()

import pandas as pd
from pymongo import MongoClient

# Leer el CSV
df = pd.read_csv("datos_kaggle/kz_cleaned.csv")

# Conexión a MongoDB
client = MongoClient("mongodb://mongodb:27017/")
db = client.kz
collection = db.ventas

# Limpiar la colección si ya tiene datos
collection.delete_many({})

# Insertar datos
collection.insert_many(df.to_dict(orient="records"))

print("✅ Datos insertados en MongoDB")


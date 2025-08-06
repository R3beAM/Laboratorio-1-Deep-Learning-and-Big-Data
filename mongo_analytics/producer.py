import os
import pandas as pd
from pymongo import MongoClient

def main():
    # Leer CSV
    csv_path = os.path.join("datos_kaggle", "kz_cleaned.csv")
    df = pd.read_csv(csv_path)

    # Conexión a MongoDB
    client = MongoClient("mongodb://mongo:27017/")
    db = client.kz
    collection = db.ventas

    # Limpiar colección antes de insertar (opcional)
    collection.delete_many({})

    # Insertar documentos
    records = df.to_dict(orient='records')
    collection.insert_many(records)
    print(f"✅ Insertados {len(records)} registros en MongoDB.")

if __name__ == "__main__":
    main()

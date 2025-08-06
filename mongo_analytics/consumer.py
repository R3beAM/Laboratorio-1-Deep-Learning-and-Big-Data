from pymongo import MongoClient
from collections import Counter
from datetime import datetime

def main():
    client = MongoClient("mongodb://mongo:27017/")
    db = client.kz
    collection = db.ventas

    # Consulta 1: categoría más vendida (por cantidad de documentos)
    pipeline_cat = [
        {"$group": {"_id": "$category_code", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    cat_result = list(collection.aggregate(pipeline_cat))
    most_sold_category = cat_result[0]['_id'] if cat_result else None

    # Consulta 2: marca que generó más ingresos brutos
    pipeline_brand = [
        {"$group": {"_id": "$brand", "total_income": {"$sum": {"$toDouble": "$price"}}}},
        {"$sort": {"total_income": -1}},
        {"$limit": 1}
    ]
    brand_result = list(collection.aggregate(pipeline_brand))
    top_brand = brand_result[0]['_id'] if brand_result else None

    # Consulta 3: mes con más ventas (UTC)
    pipeline_month = [
        {"$project": {
            "month": {"$dateToString": {"format": "%Y-%m", "date": {"$toDate": "$event_time"}}}
        }},
        {"$group": {"_id": "$month", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    month_result = list(collection.aggregate(pipeline_month))
    top_month = month_result[0]['_id'] if month_result else None

    print(f"✅ Categoría más vendida: {most_sold_category}")
    print(f"✅ Marca con más ingresos brutos: {top_brand}")
    print(f"✅ Mes con más ventas (UTC): {top_month}")

if __name__ == "__main__":
    main()

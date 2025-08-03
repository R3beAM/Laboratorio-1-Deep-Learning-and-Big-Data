from pymongo import MongoClient
import time

def main():
    client = MongoClient("mongodb://admin:admin123@localhost:27017/")
    db = client["testdb"]
    collection = db["messages"]

    last_id = None
    while True:
        query = {}
        if last_id:
            query = {"_id": {"$gt": last_id}}

        new_docs = list(collection.find(query).sort("_id", 1))

        for doc in new_docs:
            print(f"Read: {doc}")
            last_id = doc["_id"]

        time.sleep(3)  # Espera 3 segundos antes de revisar nuevos mensajes

if __name__ == "__main__":
    main()

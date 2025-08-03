from pymongo import MongoClient
import time

def main():
    client = MongoClient("mongodb://admin:admin123@localhost:27017/")
    db = client["testdb"]
    collection = db["messages"]

    count = 0
    while True:
        doc = {"message": f"Mensaje número {count}"}
        collection.insert_one(doc)
        print(f"Inserted: {doc}")
        count += 1
        time.sleep(2)  # Espera 2 segundos antes de enviar otro mensaje

if __name__ == "__main__":
    main()

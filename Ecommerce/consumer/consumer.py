import os
import redis
import json
from pymongo import MongoClient
import happybase
import time

# Redis
redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379)

# MongoDB
mongo_client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
mongo_db = mongo_client["ecommerce"]
mongo_col = mongo_db["purchase_history"]

# HBase
hbase_conn = happybase.Connection(host=os.getenv("HBASE_HOST", "localhost"), port=9090)
hbase_conn.open()
try:
    hbase_conn.create_table("purchase_history", {"info": dict()})
except:
    pass
hbase_table = hbase_conn.table("purchase_history")

def insert_hbase(record_id, data):
    hbase_table.put(record_id.encode(), {
        f"info:{k}": str(v).encode() for k, v in data.items()
    })

def consume_data():
    while True:
        item = redis_client.lpop("kz_queue")
        if item:
            record = json.loads(item)
            mongo_col.insert_one(record)
            insert_hbase(str(record.get("id", time.time())), record)
            print("Inserted one record into MongoDB and HBase")
        else:
            print("Queue empty, sleeping...")
            time.sleep(2)

if __name__ == "__main__":
    consume_data()


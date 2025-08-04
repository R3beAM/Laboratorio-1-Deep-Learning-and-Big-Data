import os
import pandas as pd
import redis
import json
import time

csv_path = os.getenv("CSV_PATH", "/data/kz_cleaned.csv")
redis_client = redis.Redis(host='redis_alt', port=6379)

def stream_data():
    df = pd.read_csv(csv_path, chunksize=1000)
    for chunk in df:
        for _, row in chunk.iterrows():
            data = row.to_dict()
            redis_client.rpush("kz_queue", json.dumps(data))
        print("Batch pushed to Redis")
        time.sleep(0.5)

if __name__ == "__main__":
    stream_data()




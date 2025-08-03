import redis
import time

r = redis.Redis(host='redis', port=6379, db=0)

for i in range(10):
    msg = f"mensaje {i}"
    r.rpush("cola", msg)
    print(f"Producer: enviado -> {msg}")
    time.sleep(1)

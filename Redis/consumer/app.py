import redis

r = redis.Redis(host='redis', port=6379, db=0)

print("Consumer: esperando mensajes...")

while True:
    _, msg = r.blpop("cola")
    print(f"Consumer: recibido -> {msg.decode()}")

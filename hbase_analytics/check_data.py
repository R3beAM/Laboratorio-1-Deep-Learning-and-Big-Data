import happybase
import time

time.sleep(5)  # Espera a que HBase esté disponible

connection = happybase.Connection(host='hbase', port=9090)
connection.open()

tables = connection.tables()
print(f"📋 Tablas en HBase: {tables}")

if b'ventas' not in tables:
    print("❌ La tabla 'ventas' no existe.")
else:
    table = connection.table('ventas')
    print("🔍 Mostrando los primeros 5 registros:")
    for i, (key, data) in enumerate(table.scan(limit=5)):
        print(f"🔑 Key: {key}")
        for k, v in data.items():
            print(f"   {k.decode()}: {v.decode(errors='ignore')}")
        if i >= 4:
            break

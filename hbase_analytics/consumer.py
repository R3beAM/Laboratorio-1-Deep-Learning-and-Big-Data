import happybase
import pandas as pd
import time

# Esperar a que HBase esté listo
time.sleep(10)

connection = happybase.Connection(host='hbase', port=9090)
connection.open()

if b'ventas' not in connection.tables():
    connection.create_table(
        b'ventas',
        {b'info': dict()}
    )

table = connection.table('ventas')

df = pd.read_csv('datos_kaggle/kz_cleaned.csv')

for idx, row in df.iterrows():
    row_key = f"row{idx}"
    table.put(row_key, {
        b'info:category': str(row['category']).encode(),
        b'info:brand': str(row['brand']).encode(),
        b'info:price': str(row['price']).encode(),
        b'info:event_time': str(row['event_time']).encode()
    })

print("✅ Datos insertados en HBase.")

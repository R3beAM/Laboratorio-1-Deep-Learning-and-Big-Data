import pandas as pd
import happybase
import time

time.sleep(10)  # Espera a que HBase esté listo

connection = happybase.Connection(host="hbase", port=9090)
connection.open()

if b'ventas' not in connection.tables():
    connection.create_table(
        b'ventas',
        {b'info': dict()}
    )

table = connection.table('ventas')
df = pd.read_csv('/data/kz_cleaned.csv')

for i, row in df.iterrows():
    table.put(f'row{i}', {
        b'info:category': str(row['category']).encode(),
        b'info:brand': str(row['brand']).encode(),
        b'info:price': str(row['price']).encode(),
        b'info:event_time': str(row['event_time']).encode(),
    })

print("✅ Datos cargados en HBase.")

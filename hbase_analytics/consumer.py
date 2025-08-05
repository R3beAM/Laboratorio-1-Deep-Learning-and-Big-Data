import os
import pandas as pd
import happybase
import time

# Esperar a que HBase esté listo
time.sleep(10)

# Ruta al archivo CSV dentro del contenedor
csv_path = os.path.join("datos_kaggle", "kz_cleaned.csv")
df = pd.read_csv(csv_path)

# Conexión con HBase
connection = happybase.Connection(host='hbase', port=9090)
connection.open()

# Crear tabla si no existe
if b'ventas' not in connection.tables():
    connection.create_table(
        b'ventas',
        {'info': dict()}
    )

table = connection.table('ventas')

# Insertar datos fila por fila
for i, row in df.iterrows():
    table.put(f"row{i}", {
        b'info:category': str(row["category_id"]).encode(),
        b'info:brand': str(row["brand"]).encode(),
        b'info:price': str(row["price"]).encode(),
        b'info:event_time': str(row["event_time"]).encode()
    })

print("✅ Datos cargados correctamente en HBase.")


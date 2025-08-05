import pandas as pd
import happybase
from collections import Counter
from datetime import datetime
import time

# Espera para que el thrift de HBase arranque
time.sleep(15)

# Conexión HBase
conn = happybase.Connection(host="hbase", port=9090)
conn.open()

# Crear tabla 'ventas' si no existe
if b'ventas' not in conn.tables():
    conn.create_table(
        b'ventas',
        {b'info': dict()}
    )

table = conn.table('ventas')

# Cargar CSV
df = pd.read_csv("kz_cleaned.csv")

# Insertar datos en HBase
for i, row in df.iterrows():
    table.put(f"row{i}".encode(), {
        b'info:category': str(row["category"]).encode(),
        b'info:brand': str(row["brand"]).encode(),
        b'info:price': str(row["price"]).encode(),
        b'info:event_time': str(row["event_time"]).encode()
    })

print("✅ Datos insertados en HBase")

# Leer datos desde HBase
categories = []
brands_income = {}
monthly_sales = Counter()

for key, data in table.scan():
    category = data.get(b'info:category', b'').decode()
    brand = data.get(b'info:brand', b'').decode()
    price = float(data.get(b'info:price', b'0').decode())
    event_time = data.get(b'info:event_time', b'').decode()

    categories.append(category)
    brands_income[brand] = brands_income.get(brand, 0) + price
    if event_time:
        dt = datetime.fromisoformat(event_time)
        month = dt.strftime("%Y-%m")
        monthly_sales[month] += 1

# Consultas
most_sold_category = Counter(categories).most_common(1)[0]
highest_income_brand = max(brands_income.items(), key=lambda x: x[1])
top_sales_month = monthly_sales.most_common(1)[0]

print(f"📦 Categoría más vendida: {most_sold_category[0]} ({most_sold_category[1]} ventas)")
print(f"💰 Marca con más ingresos: {highest_income_brand[0]} (${highest_income_brand[1]:.2f})")
print(f"📆 Mes con más ventas (UTC): {top_sales_month[0]} ({top_sales_month[1]} ventas)")

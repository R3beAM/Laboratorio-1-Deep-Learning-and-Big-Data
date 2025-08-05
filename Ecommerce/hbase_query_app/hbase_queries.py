import happybase
import pandas as pd
from collections import Counter
from datetime import datetime

# --- Conexión HBase ---
hbase = happybase.Connection(host="localhost", port=9090)
hbase.open()

# --- Crear tabla 'ventas' si no existe ---
if b'ventas' not in hbase.tables():
    print("ℹ️ Tabla 'ventas' no existe. Creándola...")
    hbase.create_table(
        b'ventas',
        {b'info': dict()}  # Familia de columnas 'info'
    )
else:
    print("✅ Tabla 'ventas' ya existe.")

# --- Acceder a la tabla ---
table = hbase.table("ventas")

# --- Cargar CSV ---
csv_path = "kz_cleaned.csv"
df = pd.read_csv(csv_path)

# --- Cargar datos solo si la tabla está vacía ---
if sum(1 for _ in table.scan()) == 0:
    print("📥 Insertando datos en HBase desde el CSV...")
    for i, row in df.iterrows():
        table.put(f"row{i}", {
            b"info:category": str(row["category"]).encode(),
            b"info:brand": str(row["brand"]).encode(),
            b"info:price": str(row["price"]).encode(),
            b"info:event_time": str(row["event_time"]).encode(),
        })
    print("✅ Datos cargados en HBase.")
else:
    print("⚠️ La tabla ya contiene datos. No se insertaron nuevos registros.")

# --- Consultas analíticas ---
category_counter = Counter()
brand_income = {}
month_counter = Counter()

print("📊 Procesando consultas analíticas...")

for _, data in table.scan():
    category = data.get(b'info:category', b'').decode()
    brand = data.get(b'info:brand', b'').decode()
    price = float(data.get(b'info:price', b'0').decode())
    event_time = data.get(b'info:event_time', b'').decode()

    category_counter[category] += 1
    brand_income[brand] = brand_income.get(brand, 0) + price

    try:
        dt = datetime.strptime(event_time, "%Y-%m-%dT%H:%M:%SZ")
        month_str = dt.strftime("%Y-%m")
        month_counter[month_str] += 1
    except ValueError:
        continue

# --- Resultados ---
most_sold_category = category_counter.most_common(1)[0]
top_brand = max(brand_income.items(), key=lambda x: x[1])
top_month = month_counter.most_common(1)[0]

print("\n🎯 Resultados de las consultas:")
print(f"1️⃣ Categoría más vendida: {most_sold_category[0]} ({most_sold_category[1]} ventas)")
print(f"2️⃣ Marca con más ingresos: {top_brand[0]} (${top_brand[1]:,.2f})")
print(f"3️⃣ Mes con más ventas: {top_month[0]} ({top_month[1]} ventas)")

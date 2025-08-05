import os
import pandas as pd
import happybase
import time
from collections import Counter

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

categories = []
brands_income = {}
monthly_sales = Counter()

# Insertar datos fila por fila
for key, data in table.scan():
    category = data.get(b'info:category_id', b'').decode()
    brand = data.get(b'info:brand', b'').decode()
    price = float(data.get(b'info:price', b'0').decode())
    event_time = data.get(b'info:event_time', b'').decode()

    # Consulta 1: categoría más vendida
    categories.append(category_id)

    # Consulta 2: ingresos brutos por marca
    brands_income[brand] = brands_income.get(brand, 0) + price

    # Consulta 3: ventas por mes (UTC)
    if event_time:
        try:
            print("Evento bruto:", event_time)
            date = datetime.fromisoformat(event_time.replace('Z', '+00:00'))
            month_key = f"{date.year}-{date.month:02d}"
            print("Mes extraído:", month_key)
            monthly_sales[month_key] += 1
        except Exception as e:
            print("Error al parsear fecha:", event_time, e)
            continue

# Resultados
if categories:
    most_common_category = Counter(categories).most_common(1)[0][0]
    print(f"✅ Categoría más vendida: {most_common_category}")
else:
    print("⚠️ No se encontraron categorías.")

if brands_income:
    top_brand = max(brands_income.items(), key=lambda x: x[1])[0]
    print(f"✅ Marca con más ingresos brutos: {top_brand}")
else:
    print("⚠️ No se encontraron marcas con ingresos.")

if monthly_sales:
    top_month = monthly_sales.most_common(1)[0][0]
    print(f"✅ Mes con más ventas (UTC): {top_month}")
else:
    print("⚠️ No se encontraron fechas válidas en los datos.")


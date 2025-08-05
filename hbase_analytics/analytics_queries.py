import happybase
import pandas as pd
from collections import Counter
from datetime import datetime
import time

# Esperar a que HBase esté listo
time.sleep(10)

connection = happybase.Connection(host='hbase', port=9090)
connection.open()
table = connection.table('ventas')

categories = []
brands_income = {}
monthly_sales = Counter()

for key, data in table.scan():
    category = data.get(b'info:category_id', b'').decode()
    brand = data.get(b'info:brand', b'').decode()
    price = float(data.get(b'info:price', b'0').decode())
    event_time = data.get(b'info:event_time', b'').decode()

    # Consulta 1: categoría más vendida
    categories.append(category)

    # Consulta 2: ingresos brutos por marca
    brands_income[brand] = brands_income.get(brand, 0) + price

    # Consulta 3: ventas por mes
    try:
        if event_time:
        # Reemplazar Z por +00:00 para que lo acepte fromisoformat
            date = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
            month_key = f"{date.year}-{date.month:02d}"
            monthly_sales[month_key] += 1
        else:
        print(f"⚠️ Campo event_time vacío para key: {key}")
    except Exception as e:
        print(f"❌ Error al parsear fecha: '{event_time}' → {e}")


# Resultados
most_common_category = Counter(categories).most_common(1)[0][0]
top_brand = max(brands_income.items(), key=lambda x: x[1])[0]
top_month = monthly_sales.most_common(1)[0][0]

print(f"✅ Categoría más vendida: {most_common_category}")
print(f"✅ Marca con más ingresos brutos: {top_brand}")
print(f"✅ Mes con más ventas (UTC): {top_month}")

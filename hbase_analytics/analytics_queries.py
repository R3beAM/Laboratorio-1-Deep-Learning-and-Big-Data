import happybase
import pandas as pd
from collections import Counter
from datetime import datetime
import time

time.sleep(10)

connection = happybase.Connection(host="hbase", port=9090)
connection.open()

table = connection.table('ventas')

categories = []
brands = []
revenues = {}
months = []

for _, data in table.scan():
    category = data.get(b'info:category', b'').decode()
    brand = data.get(b'info:brand', b'').decode()
    price = float(data.get(b'info:price', b'0').decode())
    event_time = data.get(b'info:event_time', b'').decode()

    categories.append(category)
    brands.append(brand)
    revenues[brand] = revenues.get(brand, 0) + price
    if event_time:
        try:
            dt = datetime.strptime(event_time, '%Y-%m-%d %H:%M:%S')
            months.append(dt.strftime('%Y-%m'))
        except ValueError:
            continue

# 1. Categoría más vendida
most_common_category = Counter(categories).most_common(1)[0]

# 2. Marca con más ingresos
top_brand = max(revenues.items(), key=lambda x: x[1])

# 3. Mes con más ventas
top_month = Counter(months).most_common(1)[0]

print(f"📊 Categoría más vendida: {most_common_category[0]} ({most_common_category[1]} ventas)")
print(f"💰 Marca con más ingresos: {top_brand[0]} (${top_brand[1]:.2f})")
print(f"📅 Mes con más ventas: {top_month[0]} ({top_month[1]} ventas)")

import happybase
import csv

# Asegúrate de iniciar el servidor Thrift de HBase por separado
# antes de ejecutar este script. Ejemplo:
#   hbase thrift start

# Conexión a HBase vía Thrift (puerto 9090 por defecto)
connection = happybase.Connection('localhost', port=9090)
table_name = 'ventas'

# Crear tabla si no existe
families = {
    'data': dict()
}

if table_name.encode() not in connection.tables():
    connection.create_table(table_name, families)

table = connection.table(table_name)

# Leer el CSV
with open('kz.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    
    for i, row in enumerate(reader):
        row_key = f"row{i}"
        data = {
            f"data:{key}": value
            for key, value in row.items()
        }
        table.put(row_key, data)

print("Datos cargados exitosamente en HBase.")

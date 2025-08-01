#!/usr/bin/env python3

import happybase

def connect_to_hbase(host='localhost', port=9090):
    try:
        connection = happybase.Connection(host=host, port=port)
        connection.open()
        print("✅ Conectado a HBase")
        return connection
    except Exception as e:
        print(f"❌ Error al conectar a HBase: {e}")
        return None

def consume_table(connection, table_name='mi_tabla'):
    try:
        table = connection.table(table_name)
        print(f"📄 Filas en la tabla '{table_name}':")
        for key, data in table.scan():
            print(f"🔑 Fila: {key.decode()}")
            for column, value in data.items():
                print(f"   🧬 {column.decode()}: {value.decode()}")
    except Exception as e:
        print(f"❌ Error al leer la tabla: {e}")

def main():
    connection = connect_to_hbase()
    if connection:
        consume_table(connection)
        connection.close()

if __name__ == "__main__":
    main()

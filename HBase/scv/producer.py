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

def crear_tabla_si_no_existe(connection, tabla='mi_tabla', cf='cf1'):
    tablas = connection.tables()
    if tabla.encode() not in tablas:
        print(f"🛠️ Creando tabla '{tabla}'...")
        connection.create_table(tabla, {cf: dict()})
    else:
        print(f"ℹ️ La tabla '{tabla}' ya existe.")

def insertar_datos(connection, tabla='mi_tabla'):
    table = connection.table(tabla)
    datos = {
        b'fila1': {b'cf1:nombre': b'Ana', b'cf1:edad': b'29'},
        b'fila2': {b'cf1:nombre': b'Luis', b'cf1:edad': b'34'},
        b'fila3': {b'cf1:nombre': b'Carlos', b'cf1:edad': b'42'}
    }

    for key, value in datos.items():
        table.put(key, value)
        print(f"✅ Insertada fila: {key.decode()}")

def main():
    connection = connect_to_hbase()
    if connection:
        crear_tabla_si_no_existe(connection)
        insertar_datos(connection)
        connection.close()

if __name__ == "__main__":
    main()

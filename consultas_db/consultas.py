import pandas as pd
import redis
import happybase
import pymongo
from datetime import datetime

# --- Conexión a las bases de datos (Actualiza estos datos) ---
try:
    # Conexión a Redis
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    # Conexión a HBase usando happybase
    # happybase.Connection maneja la conexión automáticamente
    hbase_connection = happybase.Connection(host='localhost', port=9090)
    
    # Conexión a MongoDB
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
    mongo_db = mongo_client['ecommerce']
    print("Conexión a bases de datos exitosa.")
except Exception as e:
    print(f"Error de conexión: {e}")
    exit()

# --- Cargar y limpiar datos ---
# La ruta es relativa a la nueva carpeta 'consultas_db'
try:
    df = pd.read_csv('../Ecommerce/datos_kaggle/kz_cleaned.csv')
    print("Datos cargados correctamente.")
    df['event_time'] = pd.to_datetime(df['event_time'], utc=True)
except FileNotFoundError:
    print("Error: El archivo kz_cleaned.csv no se encontró. Verifica la ruta.")
    exit()

# --- Funciones de consulta ---
def categoria_mas_vendida(dataframe):
    """Calcula y devuelve la categoría de producto con más ventas."""
    ventas_por_categoria = dataframe[dataframe['event_type'] == 'purchase'].groupby('category_code').size()
    return ventas_por_categoria.idxmax() if not ventas_por_categoria.empty else "N/A"

def marca_mas_ingresos(dataframe):
    """Calcula la marca con mayores ingresos brutos (suma de precios)."""
    ingresos_por_marca = dataframe.groupby('brand')['price'].sum()
    return ingresos_por_marca.idxmax() if not ingresos_por_marca.empty else "N/A"

def mes_con_mas_ventas(dataframe):
    """Identifica el mes (en UTC) con el mayor número de ventas."""
    dataframe['mes'] = dataframe['event_time'].dt.month
    ventas_por_mes = dataframe['mes'].value_counts()
    mes_max_ventas = ventas_por_mes.idxmax() if not ventas_por_mes.empty else "N/A"
    return datetime(2025, mes_max_ventas, 1).strftime('%B') if mes_max_ventas != "N/A" else "N/A"

# --- Lógica de uso de las bases de datos ---
def guardar_y_consultar_en_bases_de_datos(dataframe):
    """Ejecuta las consultas y las guarda en las bases de datos."""
    
    # Consulta 1: Categoría más vendida
    cat_vendida = categoria_mas_vendida(dataframe)
    redis_client.set('categoria_mas_vendida', cat_vendida)
    print(f"Categoría más vendida (desde Redis): {redis_client.get('categoria_mas_vendida').decode()}")

    # Consulta 2: Marca con más ingresos
    marca_ingresos = marca_mas_ingresos(dataframe)
    mongo_db.resultados.insert_one({'consulta': 'marca_mas_ingresos', 'resultado': marca_ingresos})
    resultado_mongo = mongo_db.resultados.find_one({'consulta': 'marca_mas_ingresos'})
    print(f"Marca con más ingresos (desde MongoDB): {resultado_mongo['resultado']}")

    # Consulta 3: Mes con más ventas
    mes_ventas = mes_con_mas_ventas(dataframe)
    
    # HappyBase: Creamos una tabla si no existe y guardamos el resultado
    tabla_hbase_name = b'resultados_ventas'
    if tabla_hbase_name not in hbase_connection.tables():
        hbase_connection.create_table(tabla_hbase_name, {'info': dict()})
        print(f"Tabla '{tabla_hbase_name.decode()}' creada en HBase.")
        
    tabla_hbase = hbase_connection.table(tabla_hbase_name)
    # Insertar el resultado. En HappyBase, las columnas y sus valores deben ser de tipo bytes.
    tabla_hbase.put(b'mes_con_mas_ventas', {b'info:mes': mes_ventas.encode('utf-8')})
    
    fila_hbase = tabla_hbase.row(b'mes_con_mas_ventas')
    print(f"Mes con más ventas (desde HBase): {fila_hbase[b'info:mes'].decode('utf-8')}")
    
    # Cierra la conexión de HappyBase al finalizar
    hbase_connection.close()

if __name__ == "__main__":
    guardar_y_consultar_en_bases_de_datos(df)
